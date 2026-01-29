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

package org.apache.paimon.spark.sql

import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.data.{Blob, BlobDescriptor}
import org.apache.paimon.fs.Path
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.options.Options
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.utils.UriReaderFactory

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

import java.util
import java.util.Random

class BlobTestBase extends PaimonSparkTestBase {

  private val RANDOM = new Random

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "false")
  }

  private def forEachMode(f: String => Unit): Unit = {
    Seq("append", "pk").foreach(f)
  }

  private def tableName(base: String, mode: String): String = s"${base}_$mode"

  private def blobProperties(
      mode: String,
      blobField: String,
      primaryKey: Option[String] = None,
      blobAsDescriptor: Boolean = false,
      extra: String = ""): String = {
    val baseProps =
      if (mode == "append") "'row-tracking.enabled'='true', 'data-evolution.enabled'='true'"
      else "'bucket'='-1'"
    val descriptorProp = if (blobAsDescriptor) ", 'blob-as-descriptor'='true'" else ""
    val primaryKeyProp =
      if (mode == "pk") s", 'primary-key'='${primaryKey.getOrElse("")}'" else ""
    val extraProps = if (extra.nonEmpty) s", $extra" else ""
    s"$baseProps, 'blob-field'='$blobField'$primaryKeyProp$descriptorProp$extraProps"
  }

  test("Blob: test basic") {
    forEachMode(
      mode => {
        val table = tableName("t", mode)
        withTable(table) {
          sql(
            s"CREATE TABLE $table (id INT, data STRING, picture BINARY) " +
              s"TBLPROPERTIES (${blobProperties(mode, "picture", primaryKey = Some("id"))})")
          sql(s"INSERT INTO $table VALUES (1, 'paimon', X'48656C6C6F')")

          checkAnswer(
            sql(s"SELECT * FROM $table"),
            Seq(Row(1, "paimon", Array[Byte](72, 101, 108, 108, 111)))
          )

          val expectedFiles = if (mode == "pk") 1 else 2
          checkAnswer(
            sql(s"SELECT COUNT(*) FROM `$table$$files`"),
            Seq(Row(expectedFiles))
          )
        }
      })
  }

  test("Blob: test multiple blobs") {
    forEachMode(
      mode => {
        val table = tableName("t", mode)
        withTable(table) {
          sql(
            s"CREATE TABLE $table (id INT, data STRING, pic1 BINARY, pic2 BINARY) " +
              s"TBLPROPERTIES (${blobProperties(mode, "pic1,pic2", primaryKey = Some("id"))})")
          sql(s"INSERT INTO $table VALUES (1, 'paimon', X'48656C6C6F', X'5945')")

          checkAnswer(
            sql(s"SELECT * FROM $table"),
            Seq(Row(1, "paimon", Array[Byte](72, 101, 108, 108, 111), Array[Byte](89, 69)))
          )

          val expectedFiles = if (mode == "pk") 1 else 3
          checkAnswer(
            sql(s"SELECT COUNT(*) FROM `$table$$files`"),
            Seq(Row(expectedFiles))
          )
        }
      })
  }

  test("Blob: test write blob descriptor") {
    forEachMode(
      mode => {
        val table = tableName("t", mode)
        withTable(table) {
          val blobData = new Array[Byte](1024 * 1024)
          RANDOM.nextBytes(blobData)
          val fileIO = new LocalFileIO
          val uri = "file://" + tempDBDir.toString + "/external_blob"
          try {
            val outputStream = fileIO.newOutputStream(new Path(uri), true)
            try outputStream.write(blobData)
            finally if (outputStream != null) outputStream.close()
          }

          val blobDescriptor = new BlobDescriptor(uri, 0, blobData.length)

          sql(s"CREATE TABLE $table (id INT, data STRING, picture BINARY) " +
            s"TBLPROPERTIES (${blobProperties(mode, "picture", primaryKey = Some("id"), blobAsDescriptor = true)})")
          sql(
            s"INSERT INTO $table VALUES (1, 'paimon', X'${bytesToHex(blobDescriptor.serialize())}')," +
              s"(5, 'paimon', X'${bytesToHex(blobDescriptor.serialize())}')," +
              s"(2, 'paimon', X'${bytesToHex(blobDescriptor.serialize())}')," +
              s"(3, 'paimon', X'${bytesToHex(blobDescriptor.serialize())}')," +
              s"(4, 'paimon', X'${bytesToHex(blobDescriptor.serialize())}')")
          val newDescriptorBytes =
            sql(s"SELECT picture FROM $table WHERE id = 1")
              .collect()(0)
              .get(0)
              .asInstanceOf[Array[Byte]]
          val newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes)
          val options = new Options()
          options.set("warehouse", tempDBDir.toString)
          val catalogContext = CatalogContext.create(options)
          val uriReaderFactory = new UriReaderFactory(catalogContext)
          val blob =
            Blob.fromDescriptor(uriReaderFactory.create(newBlobDescriptor.uri), blobDescriptor)
          assert(util.Arrays.equals(blobData, blob.toData))

          sql(s"ALTER TABLE $table SET TBLPROPERTIES ('blob-as-descriptor'='false')")
          checkAnswer(
            sql(s"SELECT * FROM $table WHERE id = 1"),
            Seq(Row(1, "paimon", blobData))
          )
        }
      })
  }

  test("Blob: test write blob descriptor with partition") {
    forEachMode(
      mode => {
        val table = tableName("t", mode)
        withTable(table) {
          val blobData = new Array[Byte](1024 * 1024)
          RANDOM.nextBytes(blobData)
          val fileIO = new LocalFileIO
          val uri = "file://" + tempDBDir.toString + "/external_blob"
          try {
            val outputStream = fileIO.newOutputStream(new Path(uri), true)
            try outputStream.write(blobData)
            finally if (outputStream != null) outputStream.close()
          }

          val blobDescriptor = new BlobDescriptor(uri, 0, blobData.length)
          val extraProps =
            "'comment' = 'blob table','partition.expiration-time' = '365 d'"
          sql(
            s"CREATE TABLE IF NOT EXISTS $table (\n" +
              "id STRING,\n" +
              "name STRING,\n" +
              "file_size STRING,\n" +
              "crc64 STRING,\n" +
              "modified_time STRING,\n" +
              "content BINARY\n" +
              ") \n" +
              "PARTITIONED BY (ds STRING, batch STRING) \n" +
              s"TBLPROPERTIES (${blobProperties(mode, "content", primaryKey = Some("id"), blobAsDescriptor = true, extra = extraProps)})")
          sql(
            s"INSERT OVERWRITE TABLE $table\nPARTITION(ds= '1017',batch = 'test') VALUES \n('1','paimon','1024','12345678','20241017',X'${bytesToHex(blobDescriptor.serialize())}')")
          val newDescriptorBytes =
            sql(s"SELECT content FROM $table WHERE id = '1'")
              .collect()(0)
              .get(0)
              .asInstanceOf[Array[Byte]]
          val newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes)
          val options = new Options()
          options.set("warehouse", tempDBDir.toString)
          val catalogContext = CatalogContext.create(options)
          val uriReaderFactory = new UriReaderFactory(catalogContext)
          val blob =
            Blob.fromDescriptor(uriReaderFactory.create(newBlobDescriptor.uri), blobDescriptor)
          assert(util.Arrays.equals(blobData, blob.toData))

          sql(s"ALTER TABLE $table SET TBLPROPERTIES ('blob-as-descriptor'='false')")
          checkAnswer(
            sql(s"SELECT id, name, content FROM $table WHERE id = 1"),
            Seq(Row("1", "paimon", blobData))
          )
        }
      })
  }

  test("Blob: test write blob descriptor with built-in function") {
    forEachMode(
      mode => {
        val table = tableName("t", mode)
        withTable(table) {
          val blobData = new Array[Byte](1024 * 1024)
          RANDOM.nextBytes(blobData)
          val fileIO = new LocalFileIO
          val uri = "file://" + tempDBDir.toString + "/external_blob"
          try {
            val outputStream = fileIO.newOutputStream(new Path(uri), true)
            try outputStream.write(blobData)
            finally if (outputStream != null) outputStream.close()
          }

          val blobDescriptor = new BlobDescriptor(uri, 0, blobData.length)
          val extraProps =
            "'comment' = 'blob table','partition.expiration-time' = '365 d'"
          sql(
            s"CREATE TABLE IF NOT EXISTS $table (\n" +
              "id STRING,\n" +
              "name STRING,\n" +
              "file_size STRING,\n" +
              "crc64 STRING,\n" +
              "modified_time STRING,\n" +
              "content BINARY\n" +
              ") \n" +
              "PARTITIONED BY (ds STRING, batch STRING) \n" +
              s"TBLPROPERTIES (${blobProperties(mode, "content", primaryKey = Some("id"), blobAsDescriptor = true, extra = extraProps)})")
          sql(
            s"INSERT OVERWRITE TABLE $table\nPARTITION(ds= '1017',batch = 'test') VALUES \n('1','paimon','1024','12345678','20241017', sys.path_to_descriptor('$uri'))")
          val newDescriptorBytes =
            sql(s"SELECT content FROM $table WHERE id = '1'")
              .collect()(0)
              .get(0)
              .asInstanceOf[Array[Byte]]
          val newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes)
          val options = new Options()
          options.set("warehouse", tempDBDir.toString)
          val catalogContext = CatalogContext.create(options)
          val uriReaderFactory = new UriReaderFactory(catalogContext)
          val blob =
            Blob.fromDescriptor(uriReaderFactory.create(newBlobDescriptor.uri), blobDescriptor)
          assert(util.Arrays.equals(blobData, blob.toData))

          checkAnswer(
            sql(s"SELECT sys.descriptor_to_string(content) FROM $table"),
            Seq(Row(newBlobDescriptor.toString))
          )

          sql(s"ALTER TABLE $table SET TBLPROPERTIES ('blob-as-descriptor'='false')")
          checkAnswer(
            sql(s"SELECT id, name, content FROM $table WHERE id = 1"),
            Seq(Row("1", "paimon", blobData))
          )
        }
      })
  }

  test("Blob: test compaction") {
    forEachMode(
      mode => {
        val table = tableName("t", mode)
        withTable(table) {
          sql(
            s"CREATE TABLE $table (id INT, data STRING, picture BINARY) " +
              s"TBLPROPERTIES (${blobProperties(mode, "picture", primaryKey = Some("id"))})")
          for (i <- 1 to 10) {
            sql(s"INSERT INTO $table VALUES ($i, 'paimon', X'48656C6C6F')")
          }
          sql(s"INSERT INTO $table VALUES (1, 'paimon', X'48656C6C6F')")

          val expectedFiles = if (mode == "pk") 11 else 22
          checkAnswer(
            sql(s"SELECT COUNT(*) FROM `$table$$files`"),
            Seq(Row(expectedFiles))
          )
          val expectedFilesAfterCompact = if (mode == "pk") 1 else 12
          sql(s"CALL paimon.sys.compact('$table')").collect()
          // TODO support compact blob files for pk table
//          checkAnswer(
//            sql(s"SELECT COUNT(*) FROM `$table$$files`"),
//            Seq(Row(expectedFilesAfterCompact))
//          )
//          checkAnswer(
//            sql(s"SELECT * FROM $table LIMIT 1"),
//            Seq(Row(1, "paimon", Array[Byte](72, 101, 108, 108, 111)))
//          )
        }
      })
  }

  private val HEX_ARRAY = "0123456789ABCDEF".toCharArray

  def bytesToHex(bytes: Array[Byte]): String = {
    val hexChars = new Array[Char](bytes.length * 2)
    for (j <- bytes.indices) {
      val v = bytes(j) & 0xff
      hexChars(j * 2) = HEX_ARRAY(v >>> 4)
      hexChars(j * 2 + 1) = HEX_ARRAY(v & 0x0f)
    }
    new String(hexChars)
  }
}

class BlobTestWithV2Write extends BlobTestBase {
  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "true")
  }
}
