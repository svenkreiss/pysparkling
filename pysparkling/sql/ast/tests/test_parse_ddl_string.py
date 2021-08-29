from unittest import TestCase

import pytest
from pysparkling.sql.ast.ast_to_python import parse_ddl_string

from pysparkling.sql.types import (
    ArrayType, ByteType, DateType, DecimalType, DoubleType, IntegerType, LongType, MapType,
    ShortType, StringType,
    StructField, StructType
)
from sqlparser import SqlParsingError


class TestFunctions(TestCase):
    def test_basic_entries(self):
        schema = parse_ddl_string('some_str: string, some_int: integer, some_date: date not null')
        assert schema == StructType([
            StructField('some_str', StringType(), True),
            StructField('some_int', IntegerType(), True),
            StructField('some_date', DateType(), False),
        ])
        assert str(schema) == (
            'StructType(List('
            'StructField(some_str,StringType,true),'
            'StructField(some_int,IntegerType,true),'
            'StructField(some_date,DateType,false)'
            '))'
        )

    def test_just_returning_the_type(self):
        schema = parse_ddl_string('int')
        assert schema == IntegerType()

        schema = parse_ddl_string('INT')
        assert schema == IntegerType()

    def test_byte_decimal(self):
        schema = parse_ddl_string("a: byte, b: decimal(  16 , 8   ) ")
        assert schema == StructType([
            StructField('a', ByteType(), True),
            StructField('b', DecimalType(16, 8), True),
        ])
        assert str(
            schema) == 'StructType(List(StructField(a,ByteType,true),StructField(b,DecimalType(16,8),true)))'

    def test_double_string(self):
        schema = parse_ddl_string("a DOUBLE, b STRING")
        assert schema == StructType([
            StructField('a', DoubleType(), True),
            StructField('b', StringType(), True),
        ])
        assert str(
            schema) == 'StructType(List(StructField(a,DoubleType,true),StructField(b,StringType,true)))'

    def test_array_short(self):
        schema = parse_ddl_string("a: array< short>")
        assert schema == StructType([
            StructField('a', ArrayType(ShortType()), True),
        ])
        assert str(schema) == 'StructType(List(StructField(a,ArrayType(ShortType,true),true)))'

    def test_map(self):
        schema = parse_ddl_string(" map<string , string > ")
        assert schema == MapType(StringType(), StringType(), True)
        assert str(schema) == 'MapType(StringType,StringType,true)'

    def test_array(self):
        schema = parse_ddl_string('some_str: string, arr: array<string>')
        assert schema == StructType([
            StructField('some_str', StringType(), True),
            StructField('arr', ArrayType(StringType()), True),
        ])
        assert str(schema) == 'StructType(List(' \
                              'StructField(some_str,StringType,true),' \
                              'StructField(arr,ArrayType(StringType,true),true)' \
                              '))'

    def test_nested_array(self):
        schema = parse_ddl_string('some_str: string, arr: array<array<string>>')
        assert schema == StructType([
            StructField('some_str', StringType(), True),
            StructField('arr', ArrayType(ArrayType(StringType())), True),
        ])
        assert str(schema) == 'StructType(List(' \
                              'StructField(some_str,StringType,true),' \
                              'StructField(arr,ArrayType(ArrayType(StringType,true),true),true)' \
                              '))'

    def test_alias__long_bigint(self):
        schema = parse_ddl_string('i1: long, i2: bigint')
        assert schema == StructType([
            StructField('i1', LongType(), True),
            StructField('i2', LongType(), True),
        ])
        assert str(schema) == 'StructType(List(StructField(i1,LongType,true),StructField(i2,LongType,true)))'

    # Error cases
    def test_wrong_type(self):
        with pytest.raises(SqlParsingError):
            parse_ddl_string("blabla")

    def test_comma_at_end(self):
        with pytest.raises(SqlParsingError):
            parse_ddl_string("a: int,")

    def test_unclosed_array(self):
        with pytest.raises(SqlParsingError):
            parse_ddl_string("array<int")

    def test_too_much_closed_map(self):
        with pytest.raises(SqlParsingError):
            parse_ddl_string("map<int, boolean>>")
