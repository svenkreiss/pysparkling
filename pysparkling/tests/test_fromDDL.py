"""
Most of these test cases come from pyspark.sql.types._parse_datatype_string()
"""
import pytest

from pysparkling.sql.types import (
    ArrayType, ByteType, DateType, DecimalType, DoubleType, IntegerType, LongType, MapType, ShortType, StringType,
    StructField, StructType
)
# DataType
from pysparkling.sql.utils import ParseException


def test_basic_entries():
    schema = StructType.fromDDL('some_str: string, some_int: integer, some_date: date')
    assert schema == StructType([
        StructField('some_str', StringType(), True),
        StructField('some_int', IntegerType(), True),
        StructField('some_date', DateType(), True),
    ])
    assert str(schema) == (
        'StructType(List('
        'StructField(some_str,StringType,true),'
        'StructField(some_int,IntegerType,true),'
        'StructField(some_date,DateType,true)'
        '))'
    )


def test_just_returning_the_type():
    schema = StructType.fromDDL('int')
    assert schema == IntegerType()

    schema = StructType.fromDDL('INT')
    assert schema == IntegerType()


def test_byte_decimal():
    schema = StructType.fromDDL("a: byte, b: decimal(  16 , 8   ) ")
    assert schema == StructType([
        StructField('a', ByteType(), True),
        StructField('b', DecimalType(16, 8), True),
    ])
    assert str(schema) == 'StructType(List(StructField(a,ByteType,true),StructField(b,DecimalType(16,8),true)))'


def test_double_string():
    schema = StructType.fromDDL("a DOUBLE, b STRING")
    assert schema == StructType([
        StructField('a', DoubleType(), True),
        StructField('b', StringType(), True),
    ])
    assert str(schema) == 'StructType(List(StructField(a,DoubleType,true),StructField(b,StringType,true)))'


def test_array_short():
    schema = StructType.fromDDL("a: array< short>")
    assert schema == StructType([
        StructField('a', ArrayType(ShortType()), True),
    ])
    assert str(schema) == 'StructType(List(StructField(a,ArrayType(ShortType,true),true)))'


def test_map():
    schema = StructType.fromDDL(" map<string , string > ")
    assert schema == MapType(StringType(), StringType(), True)
    assert str(schema) == 'MapType(StringType,StringType,true)'


def test_array():
    schema = StructType.fromDDL('some_str: string, arr: array<string>')
    assert schema == StructType([
        StructField('some_str', StringType(), True),
        StructField('arr', ArrayType(StringType()), True),
    ])
    assert str(schema) == 'StructType(List(' \
                          'StructField(some_str,StringType,true),' \
                          'StructField(arr,ArrayType(StringType,true),true)' \
                          '))'


def test_nested_array():
    schema = StructType.fromDDL('some_str: string, arr: array<array<string>>')
    assert schema == StructType([
        StructField('some_str', StringType(), True),
        StructField('arr', ArrayType(ArrayType(StringType())), True),
    ])
    assert str(schema) == 'StructType(List(' \
                          'StructField(some_str,StringType,true),' \
                          'StructField(arr,ArrayType(ArrayType(StringType,true),true),true)' \
                          '))'


def test_alias__long_bigint():
    schema = StructType.fromDDL('i1: long, i2: bigint')
    assert schema == StructType([
        StructField('i1', LongType(), True),
        StructField('i2', LongType(), True),
    ])
    assert str(schema) == 'StructType(List(StructField(i1,LongType,true),StructField(i2,LongType,true)))'


# Error cases
def test_wrong_type():
    with pytest.raises(ParseException):
        StructType.fromDDL("blabla")


def test_comma_at_end():
    with pytest.raises(ParseException):
        StructType.fromDDL("a: int,")


def test_unclosed_array():
    with pytest.raises(ParseException):
        StructType.fromDDL("array<int")


def test_too_much_closed_map():
    with pytest.raises(ParseException):
        StructType.fromDDL("map<int, boolean>>")
