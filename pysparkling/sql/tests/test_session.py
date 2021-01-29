from unittest import TestCase

import pytest

from pysparkling import StorageLevel
from pysparkling.sql.types import Row, StructType, StructField, LongType, StringType, DoubleType, \
    ArrayType, MapType, IntegerType, row_from_keyed_values
from pysparkling.sql.session import SparkSession
from pysparkling import Context
from pysparkling.sql.utils import require_minimum_pandas_version

try:
    require_minimum_pandas_version()
    has_pandas = True
except ImportError:
    has_pandas = False


class SessionTests(TestCase):
    spark = SparkSession(sparkContext=Context())

    def test_session_range(self):
        df = self.spark.range(3)
        self.assertEqual(df.count(), 3)
        self.assertListEqual(df.collect(), [Row(id=0), Row(id=1), Row(id=2)])
        self.assertEqual(list(df.toLocalIterator()), [Row(id=0), Row(id=1), Row(id=2)])

    def test_session_create_data_frame_from_rdd(self):
        df = self.spark.createDataFrame(self.spark.sparkContext.parallelize([
            (1, "one"),
            (2, "two"),
            (3, "three"),
        ]))
        self.assertEqual(df.count(), 3)
        self.assertListEqual(
            df.collect(),
            [Row(_1=1, _2='one'),
             Row(_1=2, _2='two'),
             Row(_1=3, _2='three')])
        self.assertEqual(
            df.schema,
            StructType([StructField("_1", LongType(), True), StructField("_2", StringType(), True)])
        )

    def test_session_create_data_frame_from_list(self):
        df = self.spark.createDataFrame([
            (1, "one"),
            (2, "two"),
            (3, "three"),
        ])
        self.assertEqual(df.count(), 3)
        self.assertListEqual(
            df.collect(),
            [Row(_1=1, _2='one'),
             Row(_1=2, _2='two'),
             Row(_1=3, _2='three')])
        self.assertEqual(
            df.schema,
            StructType([StructField("_1", LongType(), True), StructField("_2", StringType(), True)])
        )

    @pytest.mark.skipif(not has_pandas, reason='pandas is not installed')
    def test_session_create_data_frame_from_pandas_data_frame(self):
        try:
            # Pandas is an optional dependency
            # pylint: disable=import-outside-toplevel
            import pandas as pd
        except ImportError as e:
            raise ImportError("pandas is not importable") from e

        pdf = pd.DataFrame([
            (1, "one"),
            (2, "two"),
            (3, "three")
        ])

        df = self.spark.createDataFrame(pdf)

        self.assertEqual(df.count(), 3)
        self.assertListEqual(
            df.collect(),
            [Row(**{"0": 1, "1": 'one'}),
             Row(**{"0": 2, "1": 'two'}),
             Row(**{"0": 3, "2": 'three'})])
        self.assertEqual(
            df.schema,
            StructType([StructField("0", LongType(), True), StructField("1", StringType(), True)])
        )

    def test_session_create_data_frame_from_list_with_col_names(self):
        df = self.spark.createDataFrame([(0.0, [1.0, 0.8]),
                                         (1.0, [0.0, 0.0]),
                                         (2.0, [0.5, 0.5])],
                                        ["label", "features"])
        self.assertEqual(df.count(), 3)
        self.assertListEqual(
            df.collect(),
            [
                row_from_keyed_values([("label", 0.0), ("features", [1.0, 0.8])]),
                row_from_keyed_values([("label", 1.0), ("features", [0.0, 0.0])]),
                row_from_keyed_values([("label", 2.0), ("features", [0.5, 0.5])]),
            ]
        )

        self.assertEqual(
            df.schema,
            StructType([
                StructField("label", DoubleType(), True),
                StructField("features", ArrayType(DoubleType(), True), True)
            ])
        )

    def test_session_create_data_frame_from_list_with_schema(self):
        schema = StructType([StructField("map", MapType(StringType(), IntegerType()), True)])
        df = self.spark.createDataFrame([({u'a': 1},)], schema=schema)
        self.assertEqual(df.count(), 1)
        self.assertListEqual(
            df.collect(),
            [Row(map={'a': 1})]
        )
        self.assertEqual(df.schema, schema)

    def test_session_storage_level(self):
        spark = SparkSession(Context())
        df = spark.range(4, numPartitions=2)
        self.assertEqual(repr(df.storageLevel), repr(StorageLevel(False, False, False, False, 1)))
        persisted_df = df.persist()
        self.assertEqual(persisted_df.is_cached, True)
        self.assertEqual(repr(persisted_df.storageLevel), repr(StorageLevel.MEMORY_ONLY))
