import contextlib
import io
from unittest import TestCase

from parameterized import parameterized


from pysparkling.sql.ast.ast_to_python import parse_data_type
from pysparkling.sql.types import BooleanType, ByteType, ShortType, IntegerType, \
    LongType, FloatType, DoubleType, DateType, TimestampType, StringType, BinaryType, \
    DecimalType, ArrayType, MapType, StructType, StructField


class TypeParsingTest(TestCase):
    DATA_TYPE_SCENARIOS = {
        "boolean": BooleanType(),
        "tinyint": ByteType(),
        "byte": ByteType(),
        "smallint": ShortType(),
        "short": ShortType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "bigint": LongType(),
        "long": LongType(),
        "float": FloatType(),
        "real": FloatType(),
        "double": DoubleType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "string": StringType(),
        "binary": BinaryType(),
        "decimal": DecimalType(10, 0),
        "decimal(5)": DecimalType(5, 0),
        "decimal(5, 2)": DecimalType(5, 2),
        "dec": DecimalType(10, 0),
        "numeric": DecimalType(10, 0),
        "Array<string>": ArrayType(StringType()),
        "Array<int>": ArrayType(IntegerType()),
        "Map<string, int>": MapType(StringType(), IntegerType()),
        "Map < integer, String >": MapType(IntegerType(), StringType()),
        "Struct<name: string, age: int>": StructType([
            StructField(name="name", dataType=StringType()),
            StructField(name="age", dataType=IntegerType()),
        ]),
        # todo: "interval": CalendarIntervalType(),
        # todo: "char": CharType(), + with param
        # todo: "character": CharType(), + with param
        # todo: "varchar": VarChar(), + with param
    }

    @parameterized.expand(DATA_TYPE_SCENARIOS.items())
    def test_equal(self, string, data_type):
        self.assertEqual(parse_data_type(string), data_type)

    SCHEMA_SCENARIOS = {
        'some_str: string, some_int: integer, some_date: date': (
            'root\n'
            ' |-- some_str: string (nullable = true)\n'
            ' |-- some_int: integer (nullable = true)\n'
            ' |-- some_date: date (nullable = true)\n'
        ),
        'some_str: string, arr: array<string>': (
            'root\n'
            ' |-- some_str: string (nullable = true)\n'
            ' |-- arr: array (nullable = true)\n'
            ' |    |-- element: string (containsNull = true)\n'
        ),
        'some_str: string, arr: array<array<string>>': (
            'root\n'
            ' |-- some_str: string (nullable = true)\n'
            ' |-- arr: array (nullable = true)\n'
            ' |    |-- element: array (containsNull = true)\n'
            ' |    |    |-- element: string (containsNull = true)\n'
        ),
    }

    @parameterized.expand(SCHEMA_SCENARIOS.items())
    def test_dataframe_schema_parsing(self, schema, printed_schema):
        from pysparkling import Context
        from pysparkling.sql.session import SparkSession
        spark = SparkSession(Context())
        df = spark.createDataFrame([], schema=schema)

        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            df.printSchema()
        self.assertEqual(printed_schema, f.getvalue())
