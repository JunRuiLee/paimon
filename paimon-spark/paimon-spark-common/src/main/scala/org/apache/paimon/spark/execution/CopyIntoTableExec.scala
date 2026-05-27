/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.execution

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.plans.logical.{CopyFileFormat, FileFormatType, MatchByColumnName, OnErrorMode, ValidationMode}
import org.apache.paimon.spark.copyinto.{CopyLoadHistoryManager, CopyLoadRecord}
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.DataField

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.functions.{col, input_file_name, lit, when}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

case class CopyIntoTableExec(
    spark: SparkSession,
    catalog: TableCatalog,
    ident: Identifier,
    sourcePath: String,
    columns: Option[Seq[String]],
    fileFormat: CopyFileFormat,
    pattern: Option[String],
    force: Boolean,
    onError: OnErrorMode,
    matchByColumnName: MatchByColumnName,
    purge: Boolean,
    validationMode: ValidationMode,
    out: Seq[Attribute])
  extends PaimonLeafV2CommandExec {

  override def output: Seq[Attribute] = out

  override protected def run(): Seq[InternalRow] = {
    fileFormat.validateForImport()

    if (matchByColumnName != MatchByColumnName.None && columns.isDefined) {
      throw new IllegalArgumentException(
        "MATCH_BY_COLUMN_NAME and explicit column list are mutually exclusive")
    }
    if (
      matchByColumnName != MatchByColumnName.None && fileFormat.formatType == FileFormatType.CSV
    ) {
      throw new IllegalArgumentException(
        "MATCH_BY_COLUMN_NAME is not supported with CSV format because CSV files do not have column names")
    }

    val table = catalog.loadTable(ident)
    assert(table.isInstanceOf[SparkTable])
    val paimonTable = table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]
    val tableSchema = paimonTable.schema()
    val writableColumns = tableSchema.fieldNames().asScala.toSeq
    val fields = tableSchema.fields().asScala.toSeq
    val targetColumns = resolveTargetColumns(writableColumns)

    validateNonNullableDefaults(writableColumns, targetColumns, fields)

    val (filesToLoad, skippedFiles) = listAndFilterFiles(paimonTable)

    if (filesToLoad.isEmpty) {
      return buildSkippedResults(skippedFiles)
    }

    validationMode match {
      case ValidationMode.NoValidation => // fall through to existing logic
      case ValidationMode.ReturnRows(n) =>
        return runValidateReturnRows(n, paimonTable, filesToLoad)
      case ValidationMode.ReturnErrors =>
        return runValidateReturnErrors(paimonTable, filesToLoad, firstOnly = true)
      case ValidationMode.ReturnAllErrors =>
        return runValidateReturnErrors(paimonTable, filesToLoad, firstOnly = false)
    }

    onError match {
      case OnErrorMode.SkipFile =>
        runWithSkipFile(
          paimonTable,
          filesToLoad,
          skippedFiles,
          targetColumns,
          writableColumns,
          fields)
      case _ =>
        val filePaths = filesToLoad.map(_.getPath.toString)
        val readerOptions = fileFormat.toSparkReaderOptions(onError)
        val useStructuredImport = fileFormat.formatType match {
          case FileFormatType.PARQUET => true
          case _ => false
        }
        val results = if (useStructuredImport) {
          runStructuredImport(
            paimonTable,
            filePaths,
            targetColumns,
            writableColumns,
            fields,
            filesToLoad,
            skippedFiles,
            readerOptions)
        } else {
          runTextImport(
            paimonTable,
            filePaths,
            targetColumns,
            writableColumns,
            fields,
            filesToLoad,
            skippedFiles,
            readerOptions)
        }
        purgeLoadedFiles(filesToLoad)
        results
    }
  }

  private def runWithSkipFile(
      paimonTable: FileStoreTable,
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField]): Seq[InternalRow] = {
    val paimonPath = new org.apache.paimon.fs.Path(paimonTable.location().toString)
    val historyManager = new CopyLoadHistoryManager(paimonTable.fileIO(), paimonPath)
    val tableName = CopyIntoUtils.quoteIdentifier(catalog.name(), ident)
    val readerOptions = fileFormat.toSparkReaderOptions(OnErrorMode.AbortStatement)

    val results = ArrayBuffer[InternalRow]()
    val successfullyLoaded = ArrayBuffer[FileStatus]()

    filesToLoad.foreach {
      fileStatus =>
        val filePath = fileStatus.getPath.toString
        val baseName = fileStatus.getPath.getName

        val result = Try {
          val useStructuredPath = fileFormat.formatType match {
            case FileFormatType.PARQUET => true
            case _ => false
          }
          if (useStructuredPath) {
            val rawDf = readStructuredData(Array(filePath), readerOptions)
            val selectedDf =
              buildParquetDataFrame(rawDf, targetColumns, writableColumns, fields)
            validateParquetCast(rawDf, targetColumns, writableColumns, fields)
            selectedDf.write.format("paimon").mode("append").insertInto(tableName)
            val rowCount = readStructuredData(Array(filePath), readerOptions).count()
            rowCount
          } else {
            val stringSchema = buildStringSchema(targetColumns)
            val sourceDf = readSourceData(Array(filePath), stringSchema, readerOptions)
            val finalDf =
              buildFinalDataFrame(sourceDf, targetColumns, writableColumns, fields)
            val castedDf = castAndValidate(finalDf, writableColumns, fields)
            val rowCount = castedDf.count()
            castedDf.write.format("paimon").mode("append").insertInto(tableName)
            rowCount
          }
        }

        result match {
          case Success(rowCount) =>
            successfullyLoaded += fileStatus
            val snapshotId = paimonTable.snapshotManager().latestSnapshotId()
            historyManager.recordLoaded(
              CopyLoadRecord(
                filePath = filePath,
                fileSize = fileStatus.getLen,
                lastModified = fileStatus.getModificationTime,
                loadedAt = System.currentTimeMillis(),
                snapshotId = snapshotId,
                rowsLoaded = rowCount
              ))
            results += InternalRow(
              UTF8String.fromString(baseName),
              UTF8String.fromString("LOADED"),
              rowCount,
              rowCount,
              0L,
              null)
          case Failure(e) =>
            results += InternalRow(
              UTF8String.fromString(baseName),
              UTF8String.fromString("LOAD_FAILED"),
              0L,
              0L,
              1L,
              UTF8String.fromString(e.getMessage))
        }
    }

    purgeLoadedFiles(successfullyLoaded.toArray)
    results.toSeq ++ buildSkippedResults(skippedFiles)
  }

  private def runStructuredImport(
      paimonTable: FileStoreTable,
      filePaths: Array[String],
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      readerOptions: Map[String, String]): Seq[InternalRow] = {
    val rawDf = readStructuredData(filePaths, readerOptions)

    val selectedDf = buildParquetDataFrame(rawDf, targetColumns, writableColumns, fields)

    onError match {
      case OnErrorMode.Continue =>
        val allTargetCols = writableColumns.toSet ++ targetColumns.toSet
        val inputFileCol = safeTempCol("__input_file", allTargetCols)

        // Parquet CONTINUE - no parse errors, only cast errors
        val rawDfWithFile = rawDf.withColumn(inputFileCol, input_file_name())
        val totalRowsPerFile = rawDfWithFile
          .groupBy(col(inputFileCol))
          .count()
          .collect()
          .map(row => extractBaseName(row.getString(0)) -> row.getLong(1))
          .toMap

        val castResult =
          filterParquetCastErrors(
            rawDfWithFile,
            targetColumns,
            writableColumns,
            fields,
            inputFileCol)

        val tableName = CopyIntoUtils.quoteIdentifier(catalog.name(), ident)
        val goodSelected =
          buildParquetDataFrame(castResult.df, targetColumns, writableColumns, fields)
        goodSelected
          .drop(inputFileCol)
          .write
          .format("paimon")
          .mode("append")
          .insertInto(tableName)

        buildContinueResults(
          paimonTable,
          filesToLoad,
          skippedFiles,
          totalRowsPerFile,
          Map.empty,
          castResult.errorsPerFile,
          Map.empty,
          castResult.firstErrorPerFile)
      case _ =>
        validateParquetCast(rawDf, targetColumns, writableColumns, fields)
        val tableName = CopyIntoUtils.quoteIdentifier(catalog.name(), ident)
        selectedDf.write.format("paimon").mode("append").insertInto(tableName)
        recordParquetHistoryAndBuildResults(
          paimonTable,
          filesToLoad,
          skippedFiles,
          filePaths,
          readerOptions)
    }
  }

  private def buildParquetDataFrame(
      rawDf: DataFrame,
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField]): DataFrame = {
    val resolver = columnResolver
    val sourceColumns = rawDf.columns.toSeq

    val selectExprs: Seq[Column] = writableColumns.map {
      colName =>
        if (targetColumns.exists(tc => resolver(tc, colName))) {
          val srcCol = sourceColumns.find(s => resolver(s, colName))
          srcCol match {
            case Some(s) =>
              val field = fields.find(_.name() == colName).get
              val sparkType =
                org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
              col(s).cast(sparkType).as(colName)
            case None =>
              val field = fields.find(_.name() == colName).get
              val sparkType =
                org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
              lit(null).cast(sparkType).as(colName)
          }
        } else {
          val field = fields.find(_.name() == colName).get
          val defaultVal = field.defaultValue()
          if (defaultVal != null) {
            val sparkType =
              org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
            try {
              val parsed = spark.sessionState.sqlParser.parseExpression(defaultVal)
              SparkShimLoader.shim.classicApi.column(parsed).cast(sparkType).as(colName)
            } catch {
              case _: Exception => lit(null).cast(sparkType).as(colName)
            }
          } else {
            lit(null).as(colName)
          }
        }
    }
    rawDf.select(selectExprs: _*)
  }

  private def validateParquetCast(
      rawDf: DataFrame,
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField]): Unit = {
    val resolver = columnResolver
    val sourceColumns = rawDf.columns.toSeq

    val castCheckCols = ArrayBuffer[(String, String)]()
    var validationDf = rawDf
    val existingCols = rawDf.columns.toSet ++ writableColumns.toSet
    var usedCols = existingCols

    writableColumns.zip(fields).foreach {
      case (colName, field) =>
        if (targetColumns.exists(tc => resolver(tc, colName))) {
          sourceColumns.find(s => resolver(s, colName)).foreach {
            srcColName =>
              val sparkType =
                org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
              val castColName = safeTempCol("__pq_cv_" + colName, usedCols)
              usedCols += castColName
              validationDf = validationDf.withColumn(castColName, col(srcColName).cast(sparkType))
              castCheckCols += ((srcColName, castColName))
          }
        }
    }

    if (castCheckCols.nonEmpty) {
      val badCastFilter = castCheckCols
        .map { case (src, dst) => col(src).isNotNull && col(dst).isNull }
        .reduce(_ || _)
      val badRows = validationDf.filter(badCastFilter).limit(1).collect()
      if (badRows.nonEmpty) {
        val example = castCheckCols.find {
          case (src, dst) =>
            val row = badRows(0)
            val srcIdx = validationDf.schema.fieldIndex(src)
            val dstIdx = validationDf.schema.fieldIndex(dst)
            !row.isNullAt(srcIdx) && row.isNullAt(dstIdx)
        }
        throw new IllegalArgumentException(
          s"ON_ERROR = ABORT_STATEMENT: Cast failure in column '${example.map(_._1).getOrElse("unknown")}'. Source data contains values that cannot be converted to the target type.")
      }
    }
  }

  private def filterParquetCastErrors(
      rawDfWithFile: DataFrame,
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      fileCol: String): CastFilterResult = {
    val resolver = columnResolver
    val sourceColumns = rawDfWithFile.columns.toSeq.filterNot(_ == fileCol)

    val castCheckCols = ArrayBuffer[(String, String)]()
    var validationDf = rawDfWithFile
    val existingCols = rawDfWithFile.columns.toSet ++ writableColumns.toSet
    var usedCols = existingCols

    writableColumns.zip(fields).foreach {
      case (colName, field) =>
        if (targetColumns.exists(tc => resolver(tc, colName))) {
          sourceColumns.find(s => resolver(s, colName)).foreach {
            srcColName =>
              val sparkType =
                org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
              val castColName = safeTempCol("__pq_cv_" + colName, usedCols)
              usedCols += castColName
              validationDf = validationDf.withColumn(castColName, col(srcColName).cast(sparkType))
              castCheckCols += ((srcColName, castColName))
          }
        }
    }

    if (castCheckCols.nonEmpty) {
      val badCastFilter = castCheckCols
        .map { case (src, dst) => col(src).isNotNull && col(dst).isNull }
        .reduce(_ || _)

      val errorsPerFile = validationDf
        .filter(badCastFilter)
        .groupBy(col(fileCol))
        .count()
        .collect()
        .map(row => extractBaseName(row.getString(0)) -> row.getLong(1))
        .toMap

      val firstError = if (errorsPerFile.nonEmpty) {
        val badRows = validationDf.filter(badCastFilter)
        val samplePerFile = badRows.dropDuplicates(fileCol).collect()
        samplePerFile.map {
          sampleRow =>
            val fileName = extractBaseName(sampleRow.getString(sampleRow.fieldIndex(fileCol)))
            val example = castCheckCols.find {
              case (src, dst) =>
                val srcIdx = validationDf.schema.fieldIndex(src)
                val dstIdx = validationDf.schema.fieldIndex(dst)
                !sampleRow.isNullAt(srcIdx) && sampleRow.isNullAt(dstIdx)
            }
            fileName -> s"Cast failure in column '${example.map(_._1).getOrElse("unknown")}'. Source data contains values that cannot be converted to the target type."
        }.toMap
      } else Map.empty[String, String]

      val goodDf = validationDf.filter(!badCastFilter)
      val dropCols = castCheckCols.map(_._2)
      var resultDf = goodDf
      dropCols.foreach(c => resultDf = resultDf.drop(c))

      CastFilterResult(resultDf, errorsPerFile, firstError)
    } else {
      CastFilterResult(rawDfWithFile, Map.empty, Map.empty)
    }
  }

  private def recordParquetHistoryAndBuildResults(
      paimonTable: FileStoreTable,
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      filePaths: Array[String],
      readerOptions: Map[String, String]): Seq[InternalRow] = {
    val paimonPath = new org.apache.paimon.fs.Path(paimonTable.location().toString)
    val historyManager = new CopyLoadHistoryManager(paimonTable.fileIO(), paimonPath)
    val snapshotId = paimonTable.snapshotManager().latestSnapshotId()
    val loadedAt = System.currentTimeMillis()

    val countDf = readStructuredData(filePaths, readerOptions)
    val rowCounts = countDf
      .groupBy(input_file_name().as("file"))
      .count()
      .collect()

    val fileCountMap = rowCounts.map {
      row =>
        val fullPath = row.getString(0)
        val baseName = fullPath.substring(fullPath.lastIndexOf('/') + 1)
        baseName -> row.getLong(1)
    }.toMap

    val loadedResults = filesToLoad.map {
      fileStatus =>
        val baseName = fileStatus.getPath.getName
        val rowCount = fileCountMap.getOrElse(baseName, 0L)

        historyManager.recordLoaded(
          CopyLoadRecord(
            filePath = fileStatus.getPath.toString,
            fileSize = fileStatus.getLen,
            lastModified = fileStatus.getModificationTime,
            loadedAt = loadedAt,
            snapshotId = snapshotId,
            rowsLoaded = rowCount
          ))

        InternalRow(
          UTF8String.fromString(baseName),
          UTF8String.fromString("LOADED"),
          rowCount,
          rowCount,
          0L,
          null)
    }.toSeq

    val skippedResults = buildSkippedResults(skippedFiles)
    loadedResults ++ skippedResults
  }

  private def runTextImport(
      paimonTable: FileStoreTable,
      filePaths: Array[String],
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      readerOptions: Map[String, String]): Seq[InternalRow] = {
    val stringSchema = buildStringSchema(targetColumns)

    onError match {
      case OnErrorMode.Continue =>
        runTextImportContinue(
          paimonTable,
          filePaths,
          targetColumns,
          writableColumns,
          fields,
          filesToLoad,
          skippedFiles,
          readerOptions,
          stringSchema)
      case _ =>
        runTextImportAbort(
          paimonTable,
          filePaths,
          targetColumns,
          writableColumns,
          fields,
          filesToLoad,
          skippedFiles,
          readerOptions,
          stringSchema)
    }
  }

  private def runTextImportAbort(
      paimonTable: FileStoreTable,
      filePaths: Array[String],
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      readerOptions: Map[String, String],
      stringSchema: StructType): Seq[InternalRow] = {
    val sourceDf = readSourceData(filePaths, stringSchema, readerOptions)
    val finalDf = buildFinalDataFrame(sourceDf, targetColumns, writableColumns, fields)
    val castedDf = castAndValidate(finalDf, writableColumns, fields)

    val tableName = CopyIntoUtils.quoteIdentifier(catalog.name(), ident)
    castedDf.write.format("paimon").mode("append").insertInto(tableName)

    recordHistoryAndBuildResults(
      paimonTable,
      filesToLoad,
      skippedFiles,
      filePaths,
      stringSchema,
      readerOptions)
  }

  private def runTextImportContinue(
      paimonTable: FileStoreTable,
      filePaths: Array[String],
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      readerOptions: Map[String, String],
      stringSchema: StructType): Seq[InternalRow] = {
    val allTargetCols = writableColumns.toSet ++ targetColumns.toSet
    val corruptCol = safeTempCol("_corrupt_record", allTargetCols)
    val inputFileCol = safeTempCol("__input_file", allTargetCols + corruptCol)
    val schemaWithCorrupt = stringSchema.add(StructField(corruptCol, StringType, nullable = true))
    val corruptRecordOption = Map("columnNameOfCorruptRecord" -> corruptCol)

    val sourceDf = fileFormat.formatType match {
      case FileFormatType.JSON =>
        spark.read
          .options(readerOptions ++ corruptRecordOption)
          .schema(schemaWithCorrupt)
          .json(filePaths: _*)
      case _ =>
        spark.read
          .options(readerOptions ++ corruptRecordOption)
          .schema(schemaWithCorrupt)
          .csv(filePaths: _*)
    }

    val withFile = sourceDf.withColumn(inputFileCol, input_file_name())

    val totalRowsPerFile = withFile
      .groupBy(col(inputFileCol))
      .count()
      .collect()
      .map(row => extractBaseName(row.getString(0)) -> row.getLong(1))
      .toMap

    val corruptDf = withFile.filter(col(corruptCol).isNotNull)
    val validDf = withFile.filter(col(corruptCol).isNull).drop(corruptCol)

    val parseErrors = corruptDf
      .groupBy(col(inputFileCol))
      .count()
      .collect()
      .map(row => extractBaseName(row.getString(0)) -> row.getLong(1))
      .toMap

    val firstParseErrorPerFile = if (parseErrors.nonEmpty) {
      val samplesPerFile = corruptDf
        .select(col(inputFileCol), col(corruptCol))
        .dropDuplicates(inputFileCol)
        .collect()
      samplesPerFile.map {
        row => extractBaseName(row.getString(0)) -> s"Malformed record: ${row.getString(1)}"
      }.toMap
    } else Map.empty[String, String]

    var processedDf = validDf

    val nullIfVals = fileFormat.nullIfValues
    if (nullIfVals.nonEmpty) {
      stringSchema.fieldNames.foreach {
        colName =>
          processedDf = processedDf.withColumn(
            colName,
            when(col(colName).isin(nullIfVals: _*), lit(null).cast(StringType))
              .otherwise(col(colName)))
      }
    }

    if (fileFormat.emptyFieldAsNull) {
      stringSchema.fieldNames.foreach {
        colName =>
          processedDf = processedDf.withColumn(
            colName,
            when(col(colName) === lit(""), lit(null).cast(StringType))
              .otherwise(col(colName)))
      }
    }

    val finalDf =
      buildFinalDataFrame(processedDf.drop(inputFileCol), targetColumns, writableColumns, fields)
    val finalDfWithFile = finalDf.withColumn(inputFileCol, input_file_name())

    val castResult =
      castAndFilterErrors(finalDfWithFile, writableColumns, fields, inputFileCol)

    val tableName = CopyIntoUtils.quoteIdentifier(catalog.name(), ident)
    castResult.df.drop(inputFileCol).write.format("paimon").mode("append").insertInto(tableName)

    buildContinueResults(
      paimonTable,
      filesToLoad,
      skippedFiles,
      totalRowsPerFile,
      parseErrors,
      castResult.errorsPerFile,
      firstParseErrorPerFile,
      castResult.firstErrorPerFile)
  }

  private def castAndFilterErrors(
      dfWithFile: DataFrame,
      writableColumns: Seq[String],
      fields: Seq[DataField],
      fileCol: String): CastFilterResult = {
    val nonStringCastCols = ArrayBuffer[String]()
    var castedDf = dfWithFile
    writableColumns.zip(fields).foreach {
      case (colName, field) =>
        val sparkType = org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
        if (sparkType != StringType) {
          nonStringCastCols += colName
        }
    }

    if (nonStringCastCols.nonEmpty) {
      val existingCols = dfWithFile.columns.toSet ++ writableColumns.toSet
      val castColMapping = scala.collection.mutable.Map[String, String]()
      var usedCols = existingCols
      nonStringCastCols.foreach {
        colName =>
          val tempName = safeTempCol("__cv_" + colName, usedCols)
          castColMapping(colName) = tempName
          usedCols += tempName
      }
      var withValidation = dfWithFile
      nonStringCastCols.foreach {
        colName =>
          val field = fields.find(_.name() == colName).get
          val sparkType = org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
          withValidation =
            withValidation.withColumn(castColMapping(colName), col(colName).cast(sparkType))
      }
      val badCastFilter = nonStringCastCols
        .map(cn => col(cn).isNotNull && col(castColMapping(cn)).isNull)
        .reduce(_ || _)

      val badRowsDf = withValidation.filter(badCastFilter)
      val errorsPerFile = badRowsDf
        .groupBy(col(fileCol))
        .count()
        .collect()
        .map(row => extractBaseName(row.getString(0)) -> row.getLong(1))
        .toMap

      val firstError = if (errorsPerFile.nonEmpty) {
        val samplePerFile = badRowsDf.dropDuplicates(fileCol).collect()
        samplePerFile.map {
          row =>
            val fileName = extractBaseName(row.getString(row.fieldIndex(fileCol)))
            val example = nonStringCastCols.find {
              cn =>
                val srcIdx = withValidation.schema.fieldIndex(cn)
                val dstIdx = withValidation.schema.fieldIndex(castColMapping(cn))
                !row.isNullAt(srcIdx) && row.isNullAt(dstIdx)
            }
            fileName -> s"Cast failure in column '${example.getOrElse("unknown")}'. Source data contains values that cannot be converted to the target type."
        }.toMap
      } else Map.empty[String, String]

      val goodFilter = !badCastFilter
      val goodDf = withValidation.filter(goodFilter)
      val dropCols = nonStringCastCols.map(castColMapping(_))
      var resultDf = goodDf
      dropCols.foreach(c => resultDf = resultDf.drop(c))

      writableColumns.zip(fields).foreach {
        case (colName, field) =>
          val sparkType = org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
          resultDf = resultDf.withColumn(colName, col(colName).cast(sparkType))
      }

      CastFilterResult(resultDf, errorsPerFile, firstError)
    } else {
      writableColumns.zip(fields).foreach {
        case (colName, field) =>
          val sparkType = org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
          castedDf = castedDf.withColumn(colName, col(colName).cast(sparkType))
      }
      CastFilterResult(castedDf, Map.empty, Map.empty)
    }
  }

  private def buildContinueResults(
      paimonTable: FileStoreTable,
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      totalRowsPerFile: Map[String, Long],
      parseErrors: Map[String, Long],
      castErrors: Map[String, Long],
      firstParseErrorPerFile: Map[String, String],
      firstCastErrorPerFile: Map[String, String]): Seq[InternalRow] = {
    val paimonPath = new org.apache.paimon.fs.Path(paimonTable.location().toString)
    val historyManager = new CopyLoadHistoryManager(paimonTable.fileIO(), paimonPath)
    val snapshotId = paimonTable.snapshotManager().latestSnapshotId()
    val loadedAt = System.currentTimeMillis()

    val loadedResults = filesToLoad.map {
      fileStatus =>
        val baseName = fileStatus.getPath.getName
        val fullPath = fileStatus.getPath.toString
        val parsedCount = totalRowsPerFile.getOrElse(baseName, 0L)
        val fileParseErrors = parseErrors.getOrElse(baseName, 0L)
        val fileCastErrors = castErrors.getOrElse(baseName, 0L)
        val totalFileErrors = fileParseErrors + fileCastErrors
        val rowsLoaded = Math.max(0, parsedCount - totalFileErrors)

        historyManager.recordLoaded(
          CopyLoadRecord(
            filePath = fullPath,
            fileSize = fileStatus.getLen,
            lastModified = fileStatus.getModificationTime,
            loadedAt = loadedAt,
            snapshotId = snapshotId,
            rowsLoaded = rowsLoaded
          ))

        val status = if (totalFileErrors > 0) "PARTIALLY_LOADED" else "LOADED"
        val fileFirstError =
          firstParseErrorPerFile.get(baseName).orElse(firstCastErrorPerFile.get(baseName))
        InternalRow(
          UTF8String.fromString(baseName),
          UTF8String.fromString(status),
          rowsLoaded,
          parsedCount,
          totalFileErrors,
          fileFirstError.map(UTF8String.fromString).orNull
        )
    }.toSeq

    val skippedResults = buildSkippedResults(skippedFiles)
    loadedResults ++ skippedResults
  }

  private def buildStringSchema(targetColumns: Seq[String]): StructType = {
    fileFormat.formatType match {
      case FileFormatType.JSON =>
        StructType(targetColumns.map(name => StructField(name, StringType, nullable = true)))
      case _ =>
        StructType(
          (0 until targetColumns.size).map(i => StructField(s"_c$i", StringType, nullable = true)))
    }
  }

  private def resolveTargetColumns(writableColumns: Seq[String]): Seq[String] = {
    columns match {
      case Some(cols) =>
        val resolver = spark.sessionState.conf.resolver
        cols.indices.foreach {
          i =>
            cols.indices.filter(_ > i).foreach {
              j =>
                if (resolver(cols(i), cols(j))) {
                  throw new IllegalArgumentException(
                    s"Duplicate columns in column list: ${cols(i)}")
                }
            }
        }
        cols.map {
          c =>
            writableColumns.find(w => resolver(w, c)).getOrElse {
              throw new IllegalArgumentException(
                s"Column '$c' does not exist in target table. Available columns: ${writableColumns.mkString(", ")}")
            }
        }
      case None => writableColumns
    }
  }

  private def validateNonNullableDefaults(
      writableColumns: Seq[String],
      targetColumns: Seq[String],
      fields: Seq[DataField]): Unit = {
    if (columns.isEmpty) return
    val unmapped = writableColumns.filterNot(targetColumns.contains)
    unmapped.foreach {
      colName =>
        val field = fields.find(_.name() == colName).get
        if (!field.`type`().isNullable && field.defaultValue() == null) {
          throw new IllegalArgumentException(
            s"Non-nullable column '$colName' is not in the column list and has no default value")
        }
    }
  }

  private def listAndFilterFiles(
      paimonTable: FileStoreTable): (Array[FileStatus], Array[FileStatus]) = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fsPath = new Path(sourcePath)
    val fs = fsPath.getFileSystem(hadoopConf)
    val allFiles = fs.listStatus(fsPath).filter(_.isFile)

    val patternFiltered = pattern match {
      case Some(p) =>
        val regex = p.r
        allFiles.filter(f => regex.findFirstIn(f.getPath.getName).isDefined)
      case None => allFiles
    }

    if (patternFiltered.isEmpty) {
      return (Array.empty, Array.empty)
    }

    val paimonPath = new org.apache.paimon.fs.Path(paimonTable.location().toString)
    val historyManager = new CopyLoadHistoryManager(paimonTable.fileIO(), paimonPath)

    if (!force) {
      val (skip, load) = patternFiltered.partition {
        f => historyManager.isLoaded(f.getPath.toString, f.getLen, f.getModificationTime)
      }
      (load, skip)
    } else {
      (patternFiltered, Array.empty[FileStatus])
    }
  }

  private def readSourceData(
      filePaths: Array[String],
      stringSchema: StructType,
      readerOptions: Map[String, String]): DataFrame = {
    var df = fileFormat.formatType match {
      case FileFormatType.JSON =>
        spark.read.options(readerOptions).schema(stringSchema).json(filePaths: _*)
      case _ =>
        spark.read.options(readerOptions).schema(stringSchema).csv(filePaths: _*)
    }

    val nullIfVals = fileFormat.nullIfValues
    if (nullIfVals.nonEmpty) {
      df.columns.foreach {
        colName =>
          df = df.withColumn(
            colName,
            when(col(colName).isin(nullIfVals: _*), lit(null).cast(StringType))
              .otherwise(col(colName)))
      }
    }

    if (fileFormat.emptyFieldAsNull) {
      df.columns.foreach {
        colName =>
          df = df.withColumn(
            colName,
            when(col(colName) === lit(""), lit(null).cast(StringType))
              .otherwise(col(colName)))
      }
    }

    df
  }

  private def buildFinalDataFrame(
      sourceDf: DataFrame,
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField]): DataFrame = {
    val renamedDf = fileFormat.formatType match {
      case FileFormatType.JSON =>
        sourceDf
      case _ =>
        targetColumns.zipWithIndex.foldLeft(sourceDf) {
          case (df, (targetCol, idx)) => df.withColumnRenamed(s"_c$idx", targetCol)
        }
    }

    if (columns.isDefined) {
      val selectExprs: Seq[Column] = writableColumns.map {
        colName =>
          if (targetColumns.contains(colName)) {
            col(colName)
          } else {
            val field = fields.find(_.name() == colName).get
            val defaultVal = field.defaultValue()
            if (defaultVal != null) {
              val sparkType =
                org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
              try {
                val parsed = spark.sessionState.sqlParser.parseExpression(defaultVal)
                SparkShimLoader.shim.classicApi.column(parsed).cast(sparkType).as(colName)
              } catch {
                case _: Exception => lit(null).cast(sparkType).as(colName)
              }
            } else {
              lit(null).as(colName)
            }
          }
      }
      renamedDf.select(selectExprs: _*)
    } else {
      renamedDf
    }
  }

  private def castAndValidate(
      finalDf: DataFrame,
      writableColumns: Seq[String],
      fields: Seq[DataField]): DataFrame = {
    val nonStringCastCols = ArrayBuffer[String]()
    var castedDf = finalDf
    writableColumns.zip(fields).foreach {
      case (colName, field) =>
        val sparkType = org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
        castedDf = castedDf.withColumn(colName, col(colName).cast(sparkType))
        if (sparkType != StringType) {
          nonStringCastCols += colName
        }
    }

    if (nonStringCastCols.nonEmpty) {
      val existingCols = finalDf.columns.toSet ++ writableColumns.toSet
      var usedCols = existingCols
      val castColNames = nonStringCastCols.map {
        colName =>
          val tempName = safeTempCol("__cv_" + colName, usedCols)
          usedCols += tempName
          tempName
      }
      val validationDf = nonStringCastCols.zip(castColNames).foldLeft(finalDf) {
        case (df, (colName, castCol)) =>
          val field = fields.find(_.name() == colName).get
          val sparkType = org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
          df.withColumn(castCol, col(colName).cast(sparkType))
      }
      val badCastFilter = nonStringCastCols
        .zip(castColNames)
        .map { case (cn, castCol) => col(cn).isNotNull && col(castCol).isNull }
        .reduce(_ || _)
      val badRows = validationDf.filter(badCastFilter).limit(1).collect()
      if (badRows.nonEmpty) {
        val example = nonStringCastCols.zip(castColNames).find {
          case (cn, castCol) =>
            val row = badRows(0)
            val srcIdx = validationDf.schema.fieldIndex(cn)
            val dstIdx = validationDf.schema.fieldIndex(castCol)
            !row.isNullAt(srcIdx) && row.isNullAt(dstIdx)
        }
        throw new IllegalArgumentException(
          s"ON_ERROR = ABORT_STATEMENT: Cast failure in column '${example.map(_._1).getOrElse("unknown")}'. Source data contains values that cannot be converted to the target type.")
      }
    }

    castedDf
  }

  private def recordHistoryAndBuildResults(
      paimonTable: FileStoreTable,
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      filePaths: Array[String],
      stringSchema: StructType,
      readerOptions: Map[String, String]): Seq[InternalRow] = {
    val paimonPath = new org.apache.paimon.fs.Path(paimonTable.location().toString)
    val historyManager = new CopyLoadHistoryManager(paimonTable.fileIO(), paimonPath)
    val snapshotId = paimonTable.snapshotManager().latestSnapshotId()
    val loadedAt = System.currentTimeMillis()

    val countDf = fileFormat.formatType match {
      case FileFormatType.JSON =>
        spark.read.options(readerOptions).schema(stringSchema).json(filePaths: _*)
      case _ =>
        spark.read.options(readerOptions).schema(stringSchema).csv(filePaths: _*)
    }

    val rowCounts = countDf
      .groupBy(input_file_name().as("file"))
      .count()
      .collect()

    val fileCountMap = rowCounts.map {
      row =>
        val fullPath = row.getString(0)
        val baseName = fullPath.substring(fullPath.lastIndexOf('/') + 1)
        baseName -> row.getLong(1)
    }.toMap

    val loadedResults = filesToLoad.map {
      fileStatus =>
        val baseName = fileStatus.getPath.getName
        val rowCount = fileCountMap.getOrElse(baseName, 0L)

        historyManager.recordLoaded(
          CopyLoadRecord(
            filePath = fileStatus.getPath.toString,
            fileSize = fileStatus.getLen,
            lastModified = fileStatus.getModificationTime,
            loadedAt = loadedAt,
            snapshotId = snapshotId,
            rowsLoaded = rowCount
          ))

        InternalRow(
          UTF8String.fromString(baseName),
          UTF8String.fromString("LOADED"),
          rowCount,
          rowCount,
          0L,
          null)
    }.toSeq

    val skippedResults = buildSkippedResults(skippedFiles)
    loadedResults ++ skippedResults
  }

  private def runValidateReturnRows(
      n: Int,
      paimonTable: FileStoreTable,
      filesToLoad: Array[FileStatus]): Seq[InternalRow] = {
    val filePaths = filesToLoad.map(_.getPath.toString)
    val readerOptions = fileFormat.toSparkReaderOptions(OnErrorMode.AbortStatement)

    val df = fileFormat.formatType match {
      case FileFormatType.PARQUET => spark.read.options(readerOptions).parquet(filePaths: _*)
      case FileFormatType.JSON => spark.read.options(readerOptions).json(filePaths: _*)
      case _ =>
        val tableSchema = paimonTable.schema()
        val writableColumns = tableSchema.fieldNames().asScala.toSeq
        val stringSchema = StructType(
          (0 until writableColumns.size).map(
            i => StructField(s"_c$i", StringType, nullable = true)))
        spark.read.options(readerOptions).schema(stringSchema).csv(filePaths: _*)
    }

    val withFile = df.withColumn("__file__", input_file_name())
    val limited = withFile.limit(n)
    val rows = limited.collect()

    rows.zipWithIndex.map {
      case (row, idx) =>
        val fileName = row.getAs[String]("__file__")
        val baseName = new Path(fileName).getName
        val rowJson = row.toSeq.dropRight(1).mkString(", ")
        InternalRow(
          UTF8String.fromString(baseName),
          (idx + 1).toLong,
          UTF8String.fromString(rowJson))
    }.toSeq
  }

  private def runValidateReturnErrors(
      paimonTable: FileStoreTable,
      filesToLoad: Array[FileStatus],
      firstOnly: Boolean): Seq[InternalRow] = {
    val filePaths = filesToLoad.map(_.getPath.toString)
    val corruptCol = "__corrupt_record__"
    val readerOptions = fileFormat.toSparkReaderOptions(OnErrorMode.Continue) +
      ("columnNameOfCorruptRecord" -> corruptCol)

    val df = fileFormat.formatType match {
      case FileFormatType.PARQUET =>
        // Parquet has built-in schema, less likely to have parse errors
        return Seq.empty
      case FileFormatType.JSON =>
        val tableSchema = paimonTable.schema()
        val fields = tableSchema.fields().asScala.toSeq
        val sparkFields = fields.map {
          f =>
            val sparkType = org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(f.`type`())
            StructField(f.name(), sparkType, nullable = f.`type`().isNullable)
        } :+ StructField(corruptCol, StringType, nullable = true)
        val schema = StructType(sparkFields)
        spark.read.options(readerOptions).schema(schema).json(filePaths: _*)
      case _ =>
        val tableSchema = paimonTable.schema()
        val writableColumns = tableSchema.fieldNames().asScala.toSeq
        val stringSchema = StructType(
          (0 until writableColumns.size).map(i => StructField(s"_c$i", StringType, nullable = true))
            :+ StructField(corruptCol, StringType, nullable = true))
        spark.read.options(readerOptions).schema(stringSchema).csv(filePaths: _*)
    }

    val withFile = df.withColumn("__file__", input_file_name())
    val errorDf = withFile.filter(col(corruptCol).isNotNull)

    val errorRows = if (firstOnly) {
      import org.apache.spark.sql.expressions.Window
      import org.apache.spark.sql.functions.row_number
      val w = Window.partitionBy("__file__").orderBy(col(corruptCol))
      errorDf
        .withColumn("__rn__", row_number().over(w))
        .filter(col("__rn__") === 1)
        .drop("__rn__")
        .collect()
    } else {
      errorDf.collect()
    }

    errorRows.zipWithIndex.map {
      case (row, idx) =>
        val fileIdx = row.fieldIndex("__file__")
        val corruptIdx = row.fieldIndex(corruptCol)
        val fileName =
          if (!row.isNullAt(fileIdx)) new Path(row.getString(fileIdx)).getName else "unknown"
        val corruptRecord = if (!row.isNullAt(corruptIdx)) row.getString(corruptIdx) else ""
        InternalRow(
          UTF8String.fromString(fileName),
          (idx + 1).toLong,
          UTF8String.fromString("Parse error: malformed record"),
          UTF8String.fromString(corruptRecord))
    }.toSeq
  }

  private def buildSkippedResults(files: Array[FileStatus]): Seq[InternalRow] = {
    files.map {
      f =>
        InternalRow(
          UTF8String.fromString(f.getPath.getName),
          UTF8String.fromString("SKIPPED"),
          0L,
          0L,
          0L,
          null)
    }.toSeq
  }

  private def extractBaseName(fullPath: String): String = {
    fullPath.substring(fullPath.lastIndexOf('/') + 1)
  }

  /** Returns a column name resolver based on matchByColumnName setting. */
  private def columnResolver: (String, String) => Boolean = {
    matchByColumnName match {
      case MatchByColumnName.CaseSensitive => (a: String, b: String) => a == b
      case MatchByColumnName.CaseInsensitive => (a: String, b: String) => a.equalsIgnoreCase(b)
      case MatchByColumnName.None => spark.sessionState.conf.resolver
    }
  }

  /** Reads structured data (Parquet or JSON) using Spark's native schema inference. */
  private def readStructuredData(
      filePaths: Array[String],
      readerOptions: Map[String, String]): DataFrame = {
    fileFormat.formatType match {
      case FileFormatType.PARQUET =>
        spark.read.options(readerOptions).parquet(filePaths: _*)
      case FileFormatType.JSON =>
        spark.read.options(readerOptions).json(filePaths: _*)
      case other =>
        throw new IllegalArgumentException(s"readStructuredData does not support format: $other")
    }
  }

  private def safeTempCol(baseName: String, existingColumns: Set[String]): String = {
    val resolver = spark.sessionState.conf.resolver
    var candidate = baseName
    while (existingColumns.exists(c => resolver(c, candidate))) {
      candidate = "_" + candidate
    }
    candidate
  }

  private def purgeLoadedFiles(loadedFiles: Array[FileStatus]): Unit = {
    if (!purge || loadedFiles.isEmpty) return
    val hadoopConf = spark.sessionState.newHadoopConf()
    loadedFiles.foreach {
      fileStatus =>
        try {
          val path = fileStatus.getPath
          val fs = path.getFileSystem(hadoopConf)
          fs.delete(path, false)
        } catch {
          case _: Exception => // best-effort, ignore failures
        }
    }
  }
}

case class CastFilterResult(
    df: DataFrame,
    errorsPerFile: Map[String, Long],
    firstErrorPerFile: Map[String, String])
