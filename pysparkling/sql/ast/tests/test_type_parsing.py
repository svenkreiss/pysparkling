import contextlib
import io
from unittest import TestCase

from parameterized import parameterized

from pysparkling import Context
from pysparkling.sql.ast.ast_to_python import parse_data_type
from pysparkling.sql.session import SparkSession
from pysparkling.sql.types import (
    ArrayType, BinaryType, BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType,
    IntegerType, LongType,
    MapType, ShortType, StringType, StructField, StructType, TimestampType, NullType
)


class TypeParsingTest(TestCase):
    DATA_TYPE_SCENARIOS = {
        "boolean": BooleanType(),
        "booLean": BooleanType(),
        "tinyint": ByteType(),
        "byte": ByteType(),
        "smallint": ShortType(),
        "short": ShortType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "INTeger": IntegerType(),
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
        "decimal(10,5)": DecimalType(10, 5),
        "decimal(5)": DecimalType(5, 0),
        "decimal(5, 2)": DecimalType(5, 2),
        "dec": DecimalType(10, 0),
        "dec(25)": DecimalType(25, 0),
        "dec(25, 21)": DecimalType(25, 21),
        "numeric": DecimalType(10, 0),
        "Array<string>": ArrayType(StringType()),
        "Array<int>": ArrayType(IntegerType()),
        "array<doublE>": ArrayType(DoubleType()),
        "Array<map<int, tinYint>>": ArrayType(MapType(IntegerType(), ByteType(), True), True),
        "MAP<int, STRING>": MapType(IntegerType(), StringType()),
        "Map<string, int>": MapType(StringType(), IntegerType()),
        "Map < integer, String >": MapType(IntegerType(), StringType()),
        "Struct<name: string, age: int>": StructType([
            StructField(name="name", dataType=StringType()),
            StructField(name="age", dataType=IntegerType()),
        ]),
        "array<struct<tinYint:tinyint>>": ArrayType(
            StructType([StructField('tinYint', ByteType(), True)]),
            True
        ),
        "MAp<int, ARRAY<double>>": MapType(IntegerType(), ArrayType(DoubleType(), True), True),
        "MAP<int, struct<varchar:string>>": MapType(
            IntegerType(),
            StructType([StructField("varchar", StringType(), True)]),
            True
        ),
        "struct<intType: int, ts:timestamp>": StructType([
            StructField("intType", IntegerType(), True),
            StructField("ts", TimestampType(), True)
        ]),
        "Struct<int: int, timestamp:timestamp>": StructType([
            StructField("int", IntegerType(), True),
            StructField("timestamp", TimestampType(), True)
        ]),
        """
        struct<
            struct:struct<deciMal:DECimal, anotherDecimal:decimAL(5,2)>,
            MAP:Map<timestamp, varchar(10)>,
            arrAy:Array<double>,
            anotherArray:Array<char(9)>
        >
        """: (
            StructType([
                StructField("struct", StructType([
                    StructField("deciMal", DecimalType(), True),
                    StructField("anotherDecimal", DecimalType(5, 2), True)
                ]), True),
                # StructField("MAP", MapType(TimestampType(), VarcharType(10)), True),
                StructField("MAP", MapType(TimestampType(), StringType()), True),
                StructField("arrAy", ArrayType(DoubleType(), True), True),
                # StructField("anotherArray", ArrayType(CharType(9), True), True)])
                StructField("anotherArray", ArrayType(StringType(), True), True)
            ])
        ),
        "struct<`x+y`:int, `!@#$%^&*()`:string, `1_2.345<>:\"`:varchar(20)>": StructType([
            StructField("x+y", IntegerType(), True),
            StructField("!@#$%^&*()", StringType(), True),
            # StructField("1_2.345<>:\"", VarcharType(20), True)])
            StructField("1_2.345<>:\"", StringType(), True)
        ]),
        "strUCt<>": StructType([]),
        "Struct<TABLE: string, DATE:boolean>": StructType([
            StructField("TABLE", StringType(), True),
            StructField("DATE", BooleanType(), True)
        ]),
        "struct<end: long, select: int, from: string>": StructType([
            StructField("end", LongType(), True),
            StructField("select", IntegerType(), True),
            StructField("from", StringType(), True),
        ]),
        "Struct<x: INT, y: STRING COMMENT 'test'>": StructType([
            StructField("x", IntegerType(), True),
            StructField("y", StringType(), True, {'comment': 'test'}),
        ])
    }

    @parameterized.expand(DATA_TYPE_SCENARIOS.items())
    def test_equal(self, string, data_type):
        self.assertEqual(parse_data_type(string), data_type)

    def test_comment(self):
        string = (
            "Struct<"
            "x: INT Not null, "
            "y: STRING COMMENT 'nullable', "
            "z: string not null COMMENT 'test'"
            ">"
        )
        data_type = StructType([
            StructField("x", IntegerType(), False),
            StructField("y", StringType(), True, {'comment': 'nullable'}),
            StructField("z", StringType(), False, {'comment': 'test'}),
        ])
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
        spark = SparkSession(Context())
        df = spark.createDataFrame([], schema=schema)

        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            df.printSchema()
        self.assertEqual(printed_schema, f.getvalue())
