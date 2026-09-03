# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Where a bucket-scoped index file lives on disk.

Java resolves this through ``FileStorePathFactory.indexFileFactory(partition,
bucket)``, which picks one of two layouts and lets an external path override
both:

  * ``index-file-in-data-file-dir=true``  -> ``<table>/<partition>/bucket-N/``
    (``IndexInDataFileDirPathFactory``), following the data files onto an
    external path when one is configured
  * otherwise                             -> ``<table>/index/``
    (``FileStorePathFactory.globalIndexFileFactory``)

The option is immutable, so one table only ever uses one layout. Reading a table
written under the first layout with the second layout's paths raises
FileNotFoundError at index read time, well after planning succeeded.
"""

import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
import pyarrow as pa
from pypaimon import CatalogFactory, Schema
from pypaimon.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.index.deletion_vector_meta import DeletionVectorMeta
from pypaimon.index.dynamic_bucket import HASH_INDEX, DynamicBucketIndexMaintainer, _read_hashes
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.index.pk.primary_key_index_source_file import PrimaryKeyIndexSourceFile
from pypaimon.index.pk.primary_key_index_source_meta import PrimaryKeyIndexSourceMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source import primary_key_sorted_index_scan
from pypaimon.table.source.full_text_read import DataEvolutionFullTextRead
from pypaimon.table.source.primary_key_full_text_read import PrimaryKeyFullTextRead
from pypaimon.table.source.primary_key_full_text_scan import (
    PrimaryKeyFullTextScanPlan,
    PrimaryKeyFullTextSearchSplit,
)
from pypaimon.table.source.primary_key_vector_read import PrimaryKeyVectorRead
from pypaimon.table.source.primary_key_vector_scan import (
    PrimaryKeyVectorScanPlan,
    PrimaryKeyVectorSearchSplit,
)
from pypaimon.table.source.vector_search_read import DataEvolutionVectorRead
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_commit import _abort_commit_messages
from pypaimon.write.table_delete import TableDeleteByRowId

_INDEX_FILE_NAME = 'index-362ce1bd-84ac-4241-afcc-3764a10281fd-0'
_DATA_FILE_NAME = 'data-612bee3a-113e-4ea7-8854-5550674e7e0b-0.parquet'
_BUCKET = 10
_IN_DATA_FILE_DIR = {'index-file-in-data-file-dir': 'true'}


class _CapturedSplit(Exception):
    """Carries the split that reached path resolution, and stops the read there."""

    def __init__(self, split):
        super().__init__('captured')
        self.split = split


class IndexFileLayoutTestBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_table(self, table_name, partition_keys=('p_date',), **options):
        pa_schema = pa.schema([
            pa.field('p_date', pa.string(), nullable=False),
            pa.field('region', pa.string(), nullable=False),
            pa.field('pk', pa.int64(), nullable=False),
            ('value', pa.string()),
        ])
        table_options = {'bucket': '128', 'deletion-vectors.enabled': 'true'}
        table_options.update(options)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=list(partition_keys),
            primary_keys=list(partition_keys) + ['pk'],
            options=table_options,
        )
        identifier = f'default.{table_name}'
        self.catalog.create_table(identifier, schema, False)
        return self.catalog.get_table(identifier)

    def _table_root(self, table_name):
        # Anchored on the warehouse layout rather than on the path factory, so a
        # regression in the factory cannot move the expectation with it.
        return os.path.join(self.warehouse, 'default.db', table_name)

    @staticmethod
    def _dv_entry(table, partition_values=('20260831',), external_path=None):
        return IndexManifestEntry(
            kind=0,
            partition=GenericRow(list(partition_values), table.partition_keys_fields),
            bucket=_BUCKET,
            index_file=IndexFileMeta(
                index_type=IndexManifestFile.DELETION_VECTORS_INDEX,
                file_name=_INDEX_FILE_NAME,
                file_size=92,
                row_count=1,
                dv_ranges={
                    _DATA_FILE_NAME: DeletionVectorMeta(
                        data_file_name=_DATA_FILE_NAME,
                        offset=1,
                        length=83,
                        cardinality=43,
                    )
                },
                external_path=external_path,
            ),
        )


class IndexFileFactoryTest(IndexFileLayoutTestBase):
    """The layout ``FileStorePathFactory`` derives from the table's options."""

    def test_in_data_file_dir_resolves_under_the_bucket_directory(self):
        table = self._create_table('factory_in_data_dir', **_IN_DATA_FILE_DIR)
        factory = table.path_factory().index_file_factory(('20260831',), _BUCKET)

        self.assertEqual(
            os.path.join(self._table_root('factory_in_data_dir'),
                         f'p_date=20260831/bucket-{_BUCKET}'),
            factory.index_path())
        self.assertFalse(factory.is_external_path())

    def test_default_layout_resolves_under_the_table_index_directory(self):
        table = self._create_table('factory_default')
        factory = table.path_factory().index_file_factory(('20260831',), _BUCKET)

        self.assertEqual(
            os.path.join(self._table_root('factory_default'), 'index'),
            factory.index_path())

    def test_partition_directories_follow_partition_key_order(self):
        # The factory indexes the partition tuple positionally against
        # table.partition_keys, so a reordering anywhere between the manifest and
        # the factory silently produces a path that does not exist.
        table = self._create_table('factory_multi_part',
                                   partition_keys=('p_date', 'region'),
                                   **_IN_DATA_FILE_DIR)
        factory = table.path_factory().index_file_factory(('20260831', 'cn'), _BUCKET)

        self.assertEqual(
            os.path.join(self._table_root('factory_multi_part'),
                         f'p_date=20260831/region=cn/bucket-{_BUCKET}'),
            factory.index_path())

    def test_unpartitioned_table_resolves_directly_under_the_bucket_directory(self):
        table = self._create_table('factory_no_part', partition_keys=(),
                                   **_IN_DATA_FILE_DIR)
        factory = table.path_factory().index_file_factory((), _BUCKET)

        self.assertEqual(
            os.path.join(self._table_root('factory_no_part'), f'bucket-{_BUCKET}'),
            factory.index_path())

    def test_new_index_file_follows_data_files_onto_an_external_path(self):
        table = self._create_table('factory_external', **{
            'index-file-in-data-file-dir': 'true',
            'data-file.external-paths': 'oss://cold-bucket/warehouse',
            'data-file.external-paths.strategy': 'round-robin',
        })
        factory = table.path_factory().index_file_factory(('20260831',), _BUCKET)

        self.assertTrue(factory.is_external_path())
        self.assertEqual(
            f'oss://cold-bucket/warehouse/p_date=20260831/bucket-{_BUCKET}'
            f'/{_INDEX_FILE_NAME}',
            factory.to_path(_INDEX_FILE_NAME))

    def test_postpone_bucket_keeps_its_own_directory_name(self):
        table = self._create_table('factory_postpone', **{
            'bucket': '-2', 'index-file-in-data-file-dir': 'true'})
        factory = table.path_factory().index_file_factory(('20260831',), -2)

        self.assertEqual(
            os.path.join(self._table_root('factory_postpone'),
                         'p_date=20260831/bucket-postpone'),
            factory.index_path())

    def test_null_partition_value_uses_the_default_partition_name(self):
        table = self._create_table('factory_null_part', **_IN_DATA_FILE_DIR)
        factory = table.path_factory().index_file_factory((None,), _BUCKET)

        self.assertEqual(
            os.path.join(self._table_root('factory_null_part'),
                         f'p_date=__DEFAULT_PARTITION__/bucket-{_BUCKET}'),
            factory.index_path())

    def test_existing_index_falls_back_to_the_table_index_dir_not_the_global_root(self):
        # Java resolves a stored index file against indexPath(), never
        # globalIndexRootDir(), because alter table can move the global root long
        # after the file was written. Reading it from the new root would 404.
        external_root = 'oss://cold-bucket/global-index'
        table = self._create_table('factory_global_moved', **{
            'global-index.external-path': external_root})
        factory = table.path_factory().index_file_factory(('20260831',), _BUCKET)
        stored = self._dv_entry(table).index_file

        self.assertEqual(external_root, factory.global_index_root_path())
        self.assertEqual(
            os.path.join(self._table_root('factory_global_moved'),
                         'index', _INDEX_FILE_NAME),
            factory.to_path_from_meta(stored))

    def test_default_layout_keeps_index_files_off_the_external_data_paths(self):
        # Only the in-data-file-dir layout ties an index file to its data files;
        # the table-root index directory is unaffected by data-file external paths.
        table = self._create_table('factory_external_default', **{
            'data-file.external-paths': 'oss://cold-bucket/warehouse',
            'data-file.external-paths.strategy': 'round-robin',
        })
        factory = table.path_factory().index_file_factory(('20260831',), _BUCKET)

        self.assertFalse(factory.is_external_path())
        self.assertEqual(
            os.path.join(self._table_root('factory_external_default'),
                         'index', _INDEX_FILE_NAME),
            factory.to_path(_INDEX_FILE_NAME))


class DeletionVectorIndexPathTest(IndexFileLayoutTestBase):
    """Both ends of the deletion-vector path: planning a read, and writing one."""

    @staticmethod
    def _resolved_read_path(table, entry):
        scanner = FileScanner(table, manifest_scanner=lambda: [])
        return scanner._to_deletion_files(entry)[_DATA_FILE_NAME].dv_index_path

    def test_read_resolves_under_the_bucket_directory(self):
        table = self._create_table('dv_in_data_dir', **_IN_DATA_FILE_DIR)

        self.assertEqual(
            os.path.join(self._table_root('dv_in_data_dir'),
                         f'p_date=20260831/bucket-{_BUCKET}', _INDEX_FILE_NAME),
            self._resolved_read_path(table, self._dv_entry(table)))

    def test_read_resolves_under_the_table_index_directory_by_default(self):
        table = self._create_table('dv_default')

        self.assertEqual(
            os.path.join(self._table_root('dv_default'), 'index', _INDEX_FILE_NAME),
            self._resolved_read_path(table, self._dv_entry(table)))

    def test_recorded_external_path_wins_over_both_layouts(self):
        external = f'oss://cold-bucket/somewhere/{_INDEX_FILE_NAME}'
        for name, options in (('dv_external_in_data_dir', _IN_DATA_FILE_DIR),
                              ('dv_external_default', {})):
            with self.subTest(table=name):
                table = self._create_table(name, **options)
                entry = self._dv_entry(table, external_path=external)
                self.assertEqual(external, self._resolved_read_path(table, entry))

    def test_write_lands_where_a_later_read_looks_for_it(self):
        table = self._create_table('dv_write_in_data_dir', **_IN_DATA_FILE_DIR)
        bucket_path = os.path.join(self._table_root('dv_write_in_data_dir'),
                                   f'p_date=20260831/bucket-{_BUCKET}')
        # A bucket that owns a deletion vector owns data files too, so its
        # directory already exists by the time the index is written.
        table.file_io.check_or_mkdirs(bucket_path)

        deletion_vector = BitmapDeletionVector()
        deletion_vector.checked_delete(7)
        writer = TableDeleteByRowId(table)
        entry = writer._write_deletion_vector_index(
            GenericRow(['20260831'], table.partition_keys_fields),
            _BUCKET,
            {_DATA_FILE_NAME: deletion_vector},
        )

        written = os.path.join(bucket_path, entry.index_file.file_name)
        self.assertTrue(table.file_io.exists(written), written)
        self.assertIsNone(entry.index_file.external_path)
        self.assertEqual(written, writer._index_file_path(entry))

    def test_write_follows_the_external_data_path_and_records_it(self):
        # The recorded external path is the only thing that lets a later read find
        # the file: nothing in the layout says the index went to the cold tier.
        external_root = os.path.join(self.tempdir, 'cold')
        os.makedirs(external_root, exist_ok=True)
        table = self._create_table('dv_write_external', **{
            'index-file-in-data-file-dir': 'true',
            'data-file.external-paths': 'file://' + external_root,
            'data-file.external-paths.strategy': 'round-robin',
        })

        deletion_vector = BitmapDeletionVector()
        deletion_vector.checked_delete(7)
        writer = TableDeleteByRowId(table)
        entry = writer._write_deletion_vector_index(
            GenericRow(['20260831'], table.partition_keys_fields),
            _BUCKET,
            {_DATA_FILE_NAME: deletion_vector},
        )

        expected = (f'file://{external_root}/p_date=20260831/bucket-{_BUCKET}'
                    f'/{entry.index_file.file_name}')
        self.assertEqual(expected, entry.index_file.external_path)
        self.assertTrue(table.file_io.exists(expected), expected)
        self.assertEqual(expected, writer._index_file_path(entry))

    def test_default_layout_write_follows_the_global_index_external_path(self):
        # Without the bucket layout the deletion vector still goes wherever new index
        # files go, which global-index.external-path can move. Java resolves this
        # through the same globalIndexFileFactory, and the recorded external path is
        # what lets a later read find it after the root is moved again.
        external_root = os.path.join(self.tempdir, 'cold-global')
        os.makedirs(external_root, exist_ok=True)
        table = self._create_table('dv_write_global_external', **{
            'global-index.external-path': 'file://' + external_root})

        deletion_vector = BitmapDeletionVector()
        deletion_vector.checked_delete(7)
        writer = TableDeleteByRowId(table)
        entry = writer._write_deletion_vector_index(
            GenericRow(['20260831'], table.partition_keys_fields),
            _BUCKET,
            {_DATA_FILE_NAME: deletion_vector},
        )

        expected = f'file://{external_root}/{entry.index_file.file_name}'
        self.assertEqual(expected, entry.index_file.external_path)
        self.assertTrue(table.file_io.exists(expected), expected)
        self.assertEqual(expected, writer._index_file_path(entry))

    def test_existing_deletion_vectors_are_reread_from_where_they_were_written(self):
        # A second delete on the same bucket reloads the previous vector before
        # merging into it. Resolving that reread differently from the write loses
        # every earlier deletion instead of failing.
        table = self._create_table('dv_reread_in_data_dir', **_IN_DATA_FILE_DIR)
        bucket_path = os.path.join(self._table_root('dv_reread_in_data_dir'),
                                   f'p_date=20260831/bucket-{_BUCKET}')
        table.file_io.check_or_mkdirs(bucket_path)
        partition_row = GenericRow(['20260831'], table.partition_keys_fields)

        deletion_vector = BitmapDeletionVector()
        deletion_vector.checked_delete(7)
        writer = TableDeleteByRowId(table)
        entry = writer._write_deletion_vector_index(
            partition_row, _BUCKET, {_DATA_FILE_NAME: deletion_vector})

        # Stand in for the snapshot and index manifest the entry would have been
        # committed into; only the path resolution below is under test.
        table.snapshot_manager = lambda: SimpleNamespace(
            get_snapshot_by_id=lambda _id: SimpleNamespace(
                index_manifest='index-manifest-0'),
            get_latest_snapshot=lambda: None)
        original_read = IndexManifestFile.read
        IndexManifestFile.read = lambda self, name: [entry]
        try:
            entries, vectors = writer._read_existing_deletion_vectors(
                partition_row, _BUCKET, 1)
        finally:
            IndexManifestFile.read = original_read

        self.assertEqual([entry], entries)
        self.assertTrue(vectors[_DATA_FILE_NAME].is_deleted(7))


class HashIndexPathTest(IndexFileLayoutTestBase):
    """The dynamic-bucket HASH index shares the deletion vector's layout rules."""

    def test_read_resolves_under_the_bucket_directory(self):
        table = self._create_table('hash_in_data_dir', partition_keys=('p_date',),
                                   **{'bucket': '-1',
                                      'index-file-in-data-file-dir': 'true'})
        bucket_path = os.path.join(self._table_root('hash_in_data_dir'),
                                   f'p_date=20260831/bucket-{_BUCKET}')
        table.file_io.check_or_mkdirs(bucket_path)
        hashes = [3, 11, -7]
        with table.file_io.new_output_stream(
                os.path.join(bucket_path, _INDEX_FILE_NAME)) as stream:
            stream.write(b''.join(value.to_bytes(4, 'big', signed=True)
                                  for value in hashes))

        entry = IndexManifestEntry(
            kind=0,
            partition=GenericRow(['20260831'], table.partition_keys_fields),
            bucket=_BUCKET,
            index_file=IndexFileMeta(
                index_type=HASH_INDEX,
                file_name=_INDEX_FILE_NAME,
                file_size=4 * len(hashes),
                row_count=len(hashes),
            ),
        )
        self.assertEqual(set(hashes), _read_hashes(table, entry))

    def test_write_lands_where_a_later_read_looks_for_it(self):
        table = self._create_table('hash_write_in_data_dir',
                                   **{'bucket': '-1',
                                      'index-file-in-data-file-dir': 'true'})
        hashes = {5, -13, 61}

        maintainer = DynamicBucketIndexMaintainer(table)
        entry = maintainer._write_index(('20260831',), _BUCKET, hashes)

        written = os.path.join(self._table_root('hash_write_in_data_dir'),
                               f'p_date=20260831/bucket-{_BUCKET}',
                               entry.index_file.file_name)
        self.assertTrue(table.file_io.exists(written), written)
        self.assertIsNone(entry.index_file.external_path)
        self.assertEqual(hashes, _read_hashes(table, entry))

    def test_write_follows_the_external_data_path_and_records_it(self):
        external_root = os.path.join(self.tempdir, 'cold-hash')
        os.makedirs(external_root, exist_ok=True)
        table = self._create_table('hash_write_external', **{
            'bucket': '-1',
            'index-file-in-data-file-dir': 'true',
            'data-file.external-paths': 'file://' + external_root,
            'data-file.external-paths.strategy': 'round-robin',
        })
        hashes = {5, -13, 61}

        entry = DynamicBucketIndexMaintainer(table)._write_index(
            ('20260831',), _BUCKET, hashes)

        expected = (f'file://{external_root}/p_date=20260831/bucket-{_BUCKET}'
                    f'/{entry.index_file.file_name}')
        self.assertEqual(expected, entry.index_file.external_path)
        self.assertTrue(table.file_io.exists(expected), expected)
        self.assertEqual(hashes, _read_hashes(table, entry))


class PrimaryKeyIndexPathTest(IndexFileLayoutTestBase):
    """The three source-backed primary-key index families are bucket-scoped.

    Java opens all of them through indexFileFactory(partition, bucket) -- see
    IndexFileHandler.pkVectorAnnSegmentFile / pkFullTextIndexFile /
    pkSortedIndexFile. A data-evolution global index is not bucket-scoped and stays
    on the global-index root, so the shared readers must keep their old behavior.
    """

    def _split(self, table, partition_values=('20260831',), attr='data_split'):
        inner = SimpleNamespace(
            partition=GenericRow(list(partition_values), table.partition_keys_fields),
            bucket=_BUCKET)
        return SimpleNamespace(**{attr: inner})

    @staticmethod
    def _reader(cls, table):
        # __init__ wants a whole scan context (columns, query, limit) that says
        # nothing about paths; only _table matters here.
        reader = object.__new__(cls)
        reader._table = table
        return reader

    def _bucket_dir(self, table_name):
        return os.path.join(self._table_root(table_name),
                            f'p_date=20260831/bucket-{_BUCKET}')

    def test_vector_payloads_are_opened_in_the_bucket_directory(self):
        table = self._create_table('pk_vector_in_data_dir', **_IN_DATA_FILE_DIR)
        reader = self._reader(PrimaryKeyVectorRead, table)

        self.assertEqual(self._bucket_dir('pk_vector_in_data_dir'),
                         reader._index_base_path(self._split(table)))

    def test_vector_payloads_use_the_table_index_directory_by_default(self):
        table = self._create_table('pk_vector_default')
        reader = self._reader(PrimaryKeyVectorRead, table)

        self.assertEqual(
            os.path.join(self._table_root('pk_vector_default'), 'index'),
            reader._index_base_path(self._split(table)))

    def test_full_text_payloads_are_opened_in_the_bucket_directory(self):
        table = self._create_table('pk_full_text_in_data_dir', **_IN_DATA_FILE_DIR)
        reader = self._reader(PrimaryKeyFullTextRead, table)

        self.assertEqual(self._bucket_dir('pk_full_text_in_data_dir'),
                         reader._index_base_path(self._split(table)))

    def test_full_text_payloads_use_the_table_index_directory_by_default(self):
        table = self._create_table('pk_full_text_default')
        reader = self._reader(PrimaryKeyFullTextRead, table)

        self.assertEqual(
            os.path.join(self._table_root('pk_full_text_default'), 'index'),
            reader._index_base_path(self._split(table)))

    def test_sorted_index_payloads_are_opened_in_the_bucket_directory(self):
        table = self._create_table('pk_sorted_in_data_dir', **_IN_DATA_FILE_DIR)
        captured = {}

        def spy(index_type, file_io, index_path, field, io_metas, options=None):
            captured['index_path'] = index_path
            return [object()]

        field = table.fields[0]
        payload = SimpleNamespace(
            file_name=_INDEX_FILE_NAME, file_size=1, external_path=None,
            global_index_meta=SimpleNamespace(index_meta=None))
        definition = SimpleNamespace(
            field_id=field.id, index_type='PK_SORTED', options={})

        original = primary_key_sorted_index_scan._create_inner_readers
        primary_key_sorted_index_scan._create_inner_readers = spy
        try:
            create = primary_key_sorted_index_scan.reader_factory(table)
            create(self._split(table, attr='source_split'), definition, [payload])
        finally:
            primary_key_sorted_index_scan._create_inner_readers = original

        self.assertEqual(self._bucket_dir('pk_sorted_in_data_dir'),
                         captured['index_path'])

    def _payload(self):
        return SimpleNamespace(
            index_type='HNSW', file_name=_INDEX_FILE_NAME, file_size=1,
            external_path=None,
            global_index_meta=SimpleNamespace(index_meta=None))

    def _capture_split_at_path_resolution(self, reader):
        def capture(split=None):
            raise _CapturedSplit(split)
        reader._index_base_path = capture

    def test_vector_read_hands_the_split_down_to_path_resolution(self):
        # The bucket-scoped override is dead weight if _eval keeps the split to
        # itself, and a dropped argument silently falls back to the global root.
        table = self._create_table('pk_vector_handoff', **_IN_DATA_FILE_DIR)
        reader = self._reader(PrimaryKeyVectorRead, table)
        reader._vector_column = SimpleNamespace(name='value')
        reader._options = {}
        split = self._split(table)
        self._capture_split_at_path_resolution(reader)

        with self.assertRaises(_CapturedSplit) as caught:
            reader._eval(0, 9, [self._payload()], [0.0], 4, None, split)

        self.assertIs(split, caught.exception.split)

    def test_full_text_read_hands_the_split_down_to_path_resolution(self):
        table = self._create_table('pk_full_text_handoff', **_IN_DATA_FILE_DIR)
        reader = self._reader(PrimaryKeyFullTextRead, table)
        split = self._split(table)
        self._capture_split_at_path_resolution(reader)

        with self.assertRaises(_CapturedSplit) as caught:
            reader._eval(0, 9, [self._payload()], None, split)

        self.assertIs(split, caught.exception.split)

    def _pk_plan(self, plan_cls, split_cls, table, row_count=4):
        payload = IndexFileMeta(
            index_type='HNSW', file_name=_INDEX_FILE_NAME, file_size=1,
            row_count=row_count,
            global_index_meta=GlobalIndexMeta(
                row_range_start=0, row_range_end=row_count - 1, index_field_id=1,
                source_meta=PrimaryKeyIndexSourceMeta(
                    data_level=5,
                    source_files=[PrimaryKeyIndexSourceFile(
                        _DATA_FILE_NAME, row_count)],
                ).serialize()))
        data_split = SimpleNamespace(
            partition=GenericRow(['20260831'], table.partition_keys_fields),
            bucket=_BUCKET, files=[], data_deletion_files=None)
        split = split_cls(data_split=data_split, payloads=(payload,),
                          uncovered_data_files=(), row_ranges_by_file={})
        return plan_cls(1, [split]), split

    def test_vector_read_plan_hands_its_split_to_path_resolution(self):
        # read_plan is where the split is known; a reader that keeps it to itself
        # resolves against the global root and reports an empty index.
        table = self._create_table('pk_vector_plan', **_IN_DATA_FILE_DIR)
        reader = self._reader(PrimaryKeyVectorRead, table)
        reader._vector_column = SimpleNamespace(name='value')
        reader._options = {}
        reader._limit = 4
        reader._query_vector = [0.0]
        plan, split = self._pk_plan(PrimaryKeyVectorScanPlan,
                                    PrimaryKeyVectorSearchSplit, table)
        self._capture_split_at_path_resolution(reader)

        with self.assertRaises(_CapturedSplit) as caught:
            reader.read_plan(plan)

        self.assertIs(split, caught.exception.split)

    def test_full_text_read_plan_hands_its_split_to_path_resolution(self):
        table = self._create_table('pk_full_text_plan', **_IN_DATA_FILE_DIR)
        reader = self._reader(PrimaryKeyFullTextRead, table)
        plan, split = self._pk_plan(PrimaryKeyFullTextScanPlan,
                                    PrimaryKeyFullTextSearchSplit, table)
        self._capture_split_at_path_resolution(reader)

        with self.assertRaises(_CapturedSplit) as caught:
            reader.read_plan(plan)

        self.assertIs(split, caught.exception.split)

    def test_primary_key_readers_refuse_to_guess_without_a_split(self):
        # A caller that forgets to hand the split down must fail loudly rather than
        # read the global-index root and report an empty index.
        for name, cls in (('pk_vector_no_split', PrimaryKeyVectorRead),
                          ('pk_full_text_no_split', PrimaryKeyFullTextRead)):
            with self.subTest(reader=cls.__name__):
                table = self._create_table(name, **_IN_DATA_FILE_DIR)
                reader = self._reader(cls, table)
                with self.assertRaises(ValueError):
                    reader._index_base_path(None)

    def test_primary_key_readers_tolerate_a_missing_split_by_default(self):
        for name, cls in (('pk_vector_no_split_default', PrimaryKeyVectorRead),
                          ('pk_full_text_no_split_default', PrimaryKeyFullTextRead)):
            with self.subTest(reader=cls.__name__):
                table = self._create_table(name)
                reader = self._reader(cls, table)
                self.assertEqual(
                    os.path.join(self._table_root(name), 'index'),
                    reader._index_base_path(None))

    def test_data_evolution_readers_stay_on_the_global_index_root(self):
        # These read true global indexes, which the option does not move. Sharing
        # the hook with the PK readers must not drag them into the bucket directory.
        for name, cls in (('de_vector', DataEvolutionVectorRead),
                          ('de_full_text', DataEvolutionFullTextRead)):
            with self.subTest(reader=cls.__name__):
                table = self._create_table(name, **_IN_DATA_FILE_DIR)
                reader = self._reader(cls, table)
                self.assertEqual(
                    os.path.join(self._table_root(name), 'index'),
                    reader._index_base_path(self._split(table)))


class IndexFileAbortCleanupTest(IndexFileLayoutTestBase):
    """Abort has to delete the file the writer actually created.

    It walks the commit messages this process produced, so it must resolve each
    index file the way pypaimon's own writer for that index type did -- not the way
    Java would.
    """

    def _abort_one(self, table, entry):
        _abort_commit_messages(table, [CommitMessage(
            partition=tuple(entry.partition.values),
            bucket=entry.bucket,
            new_files=[],
            index_adds=[entry],
        )])

    def test_bucket_scoped_index_is_deleted_from_the_bucket_directory(self):
        table = self._create_table('abort_in_data_dir', **_IN_DATA_FILE_DIR)
        paths = self._seed_both_layouts(table, 'abort_in_data_dir')

        self._abort_one(table, self._dv_entry(table))

        self._assert_only(table, paths, deleted='bucket')

    def _seed_both_layouts(self, table, table_name):
        """Put the same file name under both candidate directories.

        Asserting only that the intended copy disappeared would also pass if cleanup
        deleted the one in the other directory as well.
        """
        paths = {}
        for key, directory in (
                ('bucket', os.path.join(self._table_root(table_name),
                                        f'p_date=20260831/bucket-{_BUCKET}')),
                ('index', os.path.join(self._table_root(table_name), 'index'))):
            table.file_io.check_or_mkdirs(directory)
            paths[key] = os.path.join(directory, _INDEX_FILE_NAME)
            with table.file_io.new_output_stream(paths[key]) as stream:
                stream.write(b'\x01')
        return paths

    def _assert_only(self, table, paths, deleted):
        for key, path in paths.items():
            if key == deleted:
                self.assertFalse(table.file_io.exists(path), path)
            else:
                self.assertTrue(table.file_io.exists(path), path)

    def test_true_global_index_is_deleted_from_the_table_index_directory(self):
        # A global index carries no primary-key source metadata, and pypaimon writes
        # it to the global-index root whatever the table's layout is, so abort must
        # look there even under this option.
        table = self._create_table('abort_global', **_IN_DATA_FILE_DIR)
        paths = self._seed_both_layouts(table, 'abort_global')

        entry = IndexManifestEntry(
            kind=0,
            partition=GenericRow(['20260831'], table.partition_keys_fields),
            bucket=0,
            index_file=IndexFileMeta(
                index_type='btree',
                file_name=_INDEX_FILE_NAME,
                file_size=1,
                row_count=1,
                global_index_meta=GlobalIndexMeta(
                    row_range_start=0, row_range_end=0, index_field_id=1),
            ),
        )
        self._abort_one(table, entry)

        self._assert_only(table, paths, deleted='index')

    def test_primary_key_payload_is_deleted_from_the_bucket_directory(self):
        # A primary-key payload also carries GlobalIndexMeta, but its source metadata
        # marks it bucket-scoped -- Java builds it through indexFileFactory. Treating
        # the presence of GlobalIndexMeta as "global" would delete nothing and leak
        # the bucket-local file.
        table = self._create_table('abort_pk_payload', **_IN_DATA_FILE_DIR)
        paths = self._seed_both_layouts(table, 'abort_pk_payload')

        entry = IndexManifestEntry(
            kind=0,
            partition=GenericRow(['20260831'], table.partition_keys_fields),
            bucket=_BUCKET,
            index_file=IndexFileMeta(
                index_type='BITMAP',
                file_name=_INDEX_FILE_NAME,
                file_size=1,
                row_count=1,
                global_index_meta=GlobalIndexMeta(
                    row_range_start=0, row_range_end=0, index_field_id=1,
                    source_meta=PrimaryKeyIndexSourceMeta(
                        data_level=5,
                        source_files=[PrimaryKeyIndexSourceFile(
                            _DATA_FILE_NAME, 1)],
                    ).serialize()),
            ),
        )
        self._abort_one(table, entry)

        self._assert_only(table, paths, deleted='bucket')


if __name__ == '__main__':
    unittest.main()
