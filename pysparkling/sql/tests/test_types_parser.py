import re

import pytest

from pysparkling.sql.parsers.types_parser import parser
from pysparkling.sql.types import (
    ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType,
    LongType, MapType, NullType, ShortType, StringType, StructField, StructType, TimestampType
)
from pysparkling.sql.utils import ParseException

# Tests copied from sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/parser/DataTypeParserSuite.scala
# commit: c62b84a


INVALID_CHARS = re.compile('[^_a-z0-9]*', flags=re.IGNORECASE)


# Dynamic creation of test methods, much like done in Scala
def checkDataType(dataTypeString: str, expectedDataType: DataType):
    name = INVALID_CHARS.sub('', dataTypeString)

    print(f"""
def test_{name}():
    assert parser.parse("{dataTypeString}") == {expectedDataType}()
    """)


def unsupported(dataTypeString: str):
    name = INVALID_CHARS.sub('', dataTypeString)

    print(f"""
def test_{name}_is_not_supported():
    with pytest.raises(ParseException):
        parser.parse("{dataTypeString}")
""")


def test_int():
    assert parser.parse("int") == IntegerType()


def test_integer():
    assert parser.parse("integer") == IntegerType()


def test_BooLean():
    assert parser.parse("BooLean") == BooleanType()


def test_tinYint():
    assert parser.parse("tinYint") == ByteType()


def test_smallINT():
    assert parser.parse("smallINT") == ShortType()


def test_INT():
    assert parser.parse("INT") == IntegerType()


def test_INTEGER():
    assert parser.parse("INTEGER") == IntegerType()


def test_bigint():
    assert parser.parse("bigint") == LongType()


def test_float():
    assert parser.parse("float") == FloatType()


def test_dOUBle():
    assert parser.parse("dOUBle") == DoubleType()


def test_decimal105():
    assert parser.parse("decimal(10, 5)") == DecimalType(10, 5)


def test_decimal():
    assert parser.parse("decimal") == DecimalType()


def test_Dec105():
    assert parser.parse("Dec(10, 5)") == DecimalType(10, 5)


def test_deC():
    assert parser.parse("deC") == DecimalType()


def test_DATE():
    assert parser.parse("DATE") == DateType()


def test_timestamp():
    assert parser.parse("timestamp") == TimestampType()


def test_string():
    assert parser.parse("string") == StringType()


# checkDataType("ChaR(5)", CharType(5))
# checkDataType("ChaRacter(5)", CharType(5))
# checkDataType("varchAr(20)", VarcharType(20))
# checkDataType("cHaR(27)", CharType(27))


def test_BINARY():
    assert parser.parse("BINARY") == BinaryType()


def test_void():
    assert parser.parse("void") == NullType()


# checkDataType("interval", CalendarIntervalType())

def test_arraydoublE():
    assert parser.parse("array<doublE>") == ArrayType(DoubleType(), True)


def test_ArraymapinttinYint():
    assert parser.parse("Array<map<int, tinYint>>") == ArrayType(MapType(IntegerType(), ByteType(), True), True)


def test_arraystructtinYinttinyint():
    assert parser.parse("array<struct<tinYint:tinyint>>") == ArrayType(
        StructType([StructField('tinYint', ByteType(), True)]),
        True
    )


def test_MAPintSTRING():
    assert parser.parse("MAP<int, STRING>") == MapType(IntegerType(), StringType(), True)


def test_MApintARRAYdouble():
    assert parser.parse("MAp<int, ARRAY<double>>") == MapType(IntegerType(), ArrayType(DoubleType(), True), True)


def test_MAPintstructvarcharstring():
    assert parser.parse("MAP<int, struct<varchar:string>>") == MapType(
        IntegerType(),
        StructType([StructField("varchar", StringType(), True)]),
        True
    )


def test_structintTypeinttstimestamp():
    assert parser.parse("struct<intType: int, ts:timestamp>") == StructType([
        StructField("intType", IntegerType(), True),
        StructField("ts", TimestampType(), True)
    ])


# // It is fine to use the data type string as the column name.
def test_Structintinttimestamptimestamp():
    assert parser.parse("Struct<int: int, timestamp:timestamp>") == StructType([
        StructField("int", IntegerType(), True),
        StructField("timestamp", TimestampType(), True)
    ])


def test_complex_struct():
    assert parser.parse("""
    struct<
        struct:struct<deciMal:DECimal, anotherDecimal:decimAL(5,2)>,
        MAP:Map<timestamp, varchar(10)>,
        arrAy:Array<double>,
        anotherArray:Array<char(9)>
    >
    """) == (
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
    )


# // Use backticks to quote column names having special characters.
def test_structxyintstring1_2345varchar20():
    assert parser.parse("struct<`x+y`:int, `!@#$%^&*()`:string, `1_2.345<>:\"`:varchar(20)>") == StructType([
        StructField("x+y", IntegerType(), True),
        StructField("!@#$%^&*()", StringType(), True),
        # StructField("1_2.345<>:\"", VarcharType(20), True)])
        StructField("1_2.345<>:\"", StringType(), True)
    ])


# // Empty struct.
def test_strUCt():
    assert parser.parse("strUCt<>") == StructType([])


def test_itisnotadatatype_is_not_supported():
    with pytest.raises(ParseException):
        parser.parse("it is not a data type")


def test_structxyint11timestamp_is_not_supported():
    with pytest.raises(ParseException):
        parser.parse("struct<x+y: int, 1.1:timestamp>")


def test_structxint_is_not_supported():
    with pytest.raises(ParseException):
        parser.parse("struct<x: int")


def test_structxintystring_is_not_supported():
    with pytest.raises(ParseException):
        parser.parse("struct<x int, y string>")


# test("Do not print empty parentheses for no params") {
# assert(intercept("unknown").getMessage.contains("unknown is not supported"))
# assert(intercept("unknown(1,2,3)").getMessage.contains("unknown(1,2,3) is not supported"))
# }
#
# // DataType parser accepts certain reserved keywords.

def test_StructTABLEstringDATEboolean():
    assert parser.parse("Struct<TABLE: string, DATE:boolean>") == StructType([
        StructField("TABLE", StringType(), True),
        StructField("DATE", BooleanType(), True)
    ])


# // Use SQL keywords.

def test_structendlongselectintfromstring():
    assert parser.parse("struct<end: long, select: int, from: string>") == StructType([
        StructField("end", LongType(), True),
        StructField("select", IntegerType(), True),
        StructField("from", StringType(), True),
    ])


# // DataType parser accepts comments.
def test_StructxINTySTRINGCOMMENTtest():
    assert parser.parse("Struct<x: INT, y: STRING COMMENT 'test'>") == StructType([
        StructField("x", IntegerType(), True),
        StructField("y", StringType(), True, {'comment': 'test'}),
    ])
