import collections
import datetime
import os
import time
from unittest import TestCase

import pytest

from pysparkling.sql.casts import cast_from_none, cast_to_array, cast_to_binary, cast_to_boolean, cast_to_byte, \
    cast_to_date, cast_to_decimal, cast_to_float, cast_to_int, cast_to_long, cast_to_map, cast_to_short, cast_to_string, \
    cast_to_struct, cast_to_timestamp, FloatType, identity
from pysparkling.sql.types import ArrayType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, \
    IntegerType, LongType, MapType, NullType, Row, StringType, StructField, StructType, TimestampType

BASE_OPTIONS = {}


@pytest.mark.skipif(not hasattr(time, 'tzset'), reason='tzset not supported on Windows')
class CastTests(TestCase):
    maxDiff = None

    def setUp(self):
        os.environ['TZ'] = 'Europe/Paris'
        time.tzset()  # pylint: disable=no-member

    def test_identity(self):
        x = object()
        self.assertEqual(identity(x, options=BASE_OPTIONS), x)

    def test_cast_from_none(self):
        self.assertEqual(cast_from_none(None, DataType(), options=BASE_OPTIONS), None)

    def test_cast_null_to_string(self):
        self.assertEqual(cast_to_string(None, NullType(), options=BASE_OPTIONS), "null")

    def test_cast_date_to_string(self):
        self.assertEqual(
            cast_to_string(datetime.date(2019, 8, 28), DateType(), options=BASE_OPTIONS),
            "2019-08-28"
        )

    def test_cast_timestamp_to_string(self):
        self.assertEqual(
            cast_to_string(
                datetime.datetime(2019, 8, 28, 13, 5, 0),
                TimestampType(),
                options=BASE_OPTIONS
            ),
            "2019-08-28 13:05:00"
        )

    def test_cast_array_to_string(self):
        self.assertEqual(
            cast_to_string(
                [[[1, None, 2], []]],
                ArrayType(ArrayType(ArrayType(IntegerType()))),
                options=BASE_OPTIONS
            ),
            "[[[1,, 2], []]]"
        )

    def test_cast_map_to_string(self):
        self.assertEqual(
            cast_to_string(
                {True: collections.OrderedDict([("one", 1), ("nothing", None), ("three", 3)])},
                MapType(
                    BooleanType(),
                    MapType(StringType(), IntegerType())
                ),
                options=BASE_OPTIONS
            ),
            "[true -> [one -> 1, nothing ->, three -> 3]]"
        )

    def test_cast_row_to_string(self):
        self.assertEqual(
            cast_to_string(
                Row(a=collections.OrderedDict([("value", None), ("b", {"c": 7})]),
                    b=None,
                    c=True,
                    d=5.2),
                StructType([
                    StructField(
                        "a",
                        MapType(
                            StringType(),
                            MapType(StringType(), LongType(), True),
                            True
                        ),
                        True
                    ),
                    StructField("b", LongType(), True),
                    StructField("c", BooleanType(), True),
                    StructField("d", DoubleType(), True)
                ]),
                options=BASE_OPTIONS
            ),
            "[[value ->, b -> [c -> 7]],, true, 5.2]"
        )

    def test_cast_true_to_boolean(self):
        self.assertEqual(
            cast_to_boolean("TrUe", StringType(), options=BASE_OPTIONS),
            True
        )

    def test_cast_false_to_boolean(self):
        self.assertEqual(
            cast_to_boolean("FalsE", StringType(), options=BASE_OPTIONS),
            False
        )

    def test_cast_random_string_to_boolean(self):
        self.assertEqual(
            cast_to_boolean("pysparkling", StringType(), options=BASE_OPTIONS),
            None
        )

    def test_cast_empty_string_to_boolean(self):
        self.assertEqual(
            cast_to_boolean("", StringType(), options=BASE_OPTIONS),
            None
        )

    def test_cast_falsish_to_boolean(self):
        self.assertEqual(
            cast_to_boolean(0, IntegerType(), options=BASE_OPTIONS),
            False
        )

    def test_cast_truish_to_boolean(self):
        self.assertEqual(
            cast_to_boolean(-1, IntegerType(), options=BASE_OPTIONS),
            True
        )

    def test_cast_date_to_byte(self):
        self.assertEqual(
            cast_to_byte(datetime.date(2019, 8, 28), DateType(), options=BASE_OPTIONS),
            None
        )

    def test_cast_small_string_to_byte(self):
        self.assertEqual(
            cast_to_byte("-127", StringType(), options=BASE_OPTIONS),
            -127
        )

    def test_cast_bigger_string_to_byte(self):
        self.assertEqual(
            cast_to_byte("-1024", StringType(), options=BASE_OPTIONS),
            None
        )

    def test_cast_float_to_byte(self):
        self.assertEqual(
            cast_to_byte(-128.8, FloatType(), options=BASE_OPTIONS),
            -128
        )

    def test_cast_float_to_byte_with_loop(self):
        self.assertEqual(
            cast_to_byte(-730.8, FloatType(), options=BASE_OPTIONS),
            38
        )

    def test_cast_small_string_to_short(self):
        self.assertEqual(
            cast_to_short("32767", StringType(), options=BASE_OPTIONS),
            32767
        )

    def test_cast_bigger_string_to_short(self):
        self.assertEqual(
            cast_to_short("32768", StringType(), options=BASE_OPTIONS),
            None
        )

    def test_cast_float_to_short(self):
        self.assertEqual(
            cast_to_short(32767, FloatType(), options=BASE_OPTIONS),
            32767
        )

    def test_cast_float_to_short_with_loop(self):
        self.assertEqual(
            cast_to_short(32768, FloatType(), options=BASE_OPTIONS),
            -32768
        )

    def test_cast_small_string_to_int(self):
        self.assertEqual(
            cast_to_int("2147483647", StringType(), options=BASE_OPTIONS),
            2147483647
        )

    def test_cast_bigger_string_to_int(self):
        self.assertEqual(
            cast_to_int("2147483648", StringType(), options=BASE_OPTIONS),
            None
        )

    def test_cast_float_to_int(self):
        self.assertEqual(
            cast_to_int(2147483647, LongType(), options=BASE_OPTIONS),
            2147483647
        )

    def test_cast_float_to_int_with_loop(self):
        self.assertEqual(
            cast_to_int(2147483648, LongType(), options=BASE_OPTIONS),
            -2147483648
        )

    def test_cast_small_string_to_long(self):
        self.assertEqual(
            cast_to_long("9223372036854775807", StringType(), options=BASE_OPTIONS),
            9223372036854775807
        )

    def test_cast_bigger_string_to_long(self):
        self.assertEqual(
            cast_to_long("9223372036854775808", StringType(), options=BASE_OPTIONS),
            None
        )

    def test_cast_float_to_long(self):
        self.assertEqual(
            cast_to_long(9223372036854775807, LongType(), options=BASE_OPTIONS),
            9223372036854775807
        )

    def test_cast_timestamp_to_float_without_jump_issue(self):
        # Spark's floats have precision issue.
        # As pysparkling is using python that does not have this issue,
        # there is a discrepancy in behaviours
        # This test is using a value for which Spark can handle the exact value
        # Hence the behaviour is the same in pysparkling and PySpark
        self.assertEqual(
            cast_to_float(
                datetime.datetime(2019, 8, 28, 0, 2, 40),
                TimestampType(),
                options=BASE_OPTIONS
            ),
            1566943360.0
        )

    def test_cast_string_to_binary(self):
        self.assertEqual(
            cast_to_binary(
                "test",
                StringType(),
                options=BASE_OPTIONS
            ),
            bytearray(b'test')
        )

    def test_cast_year_as_string_to_date(self):
        self.assertEqual(
            cast_to_date(
                "2019",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.date(2019, 1, 1)
        )

    def test_cast_year_month_as_string_to_date(self):
        self.assertEqual(
            cast_to_date(
                "2019-02",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.date(2019, 2, 1)
        )

    def test_cast_date_as_string_to_date(self):
        self.assertEqual(
            cast_to_date(
                "2019-03-01",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.date(2019, 3, 1)
        )

    def test_cast_date_without_0_as_string_to_date(self):
        self.assertEqual(
            cast_to_date(
                "2019-4-1",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.date(2019, 4, 1)
        )

    def test_cast_weird_strings_to_date(self):
        # Mimic Spark behavior
        self.assertEqual(
            cast_to_date(
                "2019-10-0001Tthis should be ignored",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.date(2019, 10, 1)
        )

    def test_cast_basic_string_to_timestamp(self):
        self.assertEqual(
            cast_to_timestamp(
                "2019-10-01T05:40:36",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.datetime(2019, 10, 1, 5, 40, 36)
        )

    def test_cast_gmt_string_to_timestamp(self):
        self.assertEqual(
            cast_to_timestamp(
                "2019-10-01T05:40:36Z",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.datetime(2019, 10, 1, 7, 40, 36)
        )

    def test_cast_weird_tz_string_to_timestamp(self):
        self.assertEqual(
            cast_to_timestamp(
                "2019-10-01T05:40:36+3:5",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.datetime(2019, 10, 1, 4, 35, 36)
        )

    def test_cast_short_tz_string_to_timestamp(self):
        self.assertEqual(
            cast_to_timestamp(
                "2019-10-01T05:40:36+03",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.datetime(2019, 10, 1, 4, 40, 36)
        )

    def test_cast_longer_tz_string_to_timestamp(self):
        self.assertEqual(
            cast_to_timestamp(
                "2019-10-01T05:40:36+03:",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.datetime(2019, 10, 1, 4, 40, 36)
        )

    def test_cast_date_string_to_timestamp(self):
        self.assertEqual(
            cast_to_timestamp(
                "2019-10-01",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.datetime(2019, 10, 1, 0, 0, 0)
        )

    def test_cast_time_string_to_timestamp(self):
        today = datetime.date.today()
        self.assertEqual(
            cast_to_timestamp(
                "10:50:39",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.datetime(today.year, today.month, today.day, 10, 50, 39)
        )

    def test_cast_time_without_seconds_string_to_timestamp(self):
        today = datetime.date.today()
        self.assertEqual(
            cast_to_timestamp(
                "10:50",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.datetime(today.year, today.month, today.day, 10, 50, 0)
        )

    def test_cast_time_without_minutes_string_to_timestamp(self):
        today = datetime.date.today()
        self.assertEqual(
            cast_to_timestamp(
                "10::37",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.datetime(today.year, today.month, today.day, 10, 0, 37)
        )

    def test_cast_hour_string_to_timestamp(self):
        today = datetime.date.today()

        self.assertEqual(
            cast_to_timestamp(
                "10:",
                StringType(),
                options=BASE_OPTIONS
            ),
            datetime.datetime(today.year, today.month, today.day, 10, 0, 0)
        )

    def test_cast_bool_to_timestamp(self):
        self.assertEqual(
            cast_to_timestamp(
                True,
                BooleanType(),
                options=BASE_OPTIONS
            ),
            datetime.datetime(1970, 1, 1, 1, 0, 1, 0)
        )

    def test_cast_int_to_timestamp(self):
        self.assertEqual(
            cast_to_timestamp(
                86400 * 365,
                IntegerType(),
                options=BASE_OPTIONS
            ),
            datetime.datetime(1971, 1, 1, 1, 0, 0, 0)
        )

    def test_cast_decimal_to_timestamp(self):
        self.assertEqual(
            cast_to_timestamp(
                147.58,
                DecimalType(),
                options=BASE_OPTIONS
            ),
            datetime.datetime(1970, 1, 1, 1, 2, 27, 580000)
        )

    def test_cast_date_to_decimal(self):
        self.assertEqual(
            cast_to_decimal(
                datetime.date(2019, 8, 28),
                DateType(),
                DecimalType(),
                options=BASE_OPTIONS
            ),
            None
        )

    def test_cast_timestamp_to_decimal_without_scale(self):
        self.assertEqual(
            cast_to_decimal(
                datetime.datetime(2019, 8, 28),
                TimestampType(),
                DecimalType(),
                options=BASE_OPTIONS
            ),
            1566943200.0
        )

    def test_cast_timestamp_to_decimal_with_too_small_precision(self):
        self.assertEqual(
            cast_to_decimal(
                datetime.datetime(2019, 8, 28),
                TimestampType(),
                DecimalType(precision=10, scale=1),
                options=BASE_OPTIONS
            ),
            None
        )

    def test_cast_timestamp_to_decimal_with_scale(self):
        self.assertEqual(
            cast_to_decimal(
                datetime.datetime(2019, 8, 28),
                TimestampType(),
                DecimalType(precision=11, scale=1),
                options=BASE_OPTIONS
            ),
            1566943200.0
        )

    def test_cast_float_to_decimal_with_scale(self):
        self.assertEqual(
            cast_to_decimal(
                10.123456789,
                FloatType(),
                DecimalType(precision=10, scale=8),
                options=BASE_OPTIONS
            ),
            10.12345679
        )

    def test_cast_float_to_decimal_with_scale_and_other_rounding(self):
        self.assertEqual(
            cast_to_decimal(
                10.987654321,
                FloatType(),
                DecimalType(precision=10, scale=8),
                options=BASE_OPTIONS
            ),
            10.98765432
        )

    def test_cast_from_decimal_to_decimal(self):
        self.assertEqual(
            cast_to_decimal(
                cast_to_decimal(
                    1.526,
                    FloatType(),
                    DecimalType(scale=2),
                    options=BASE_OPTIONS
                ),
                DecimalType(scale=2),
                DecimalType(scale=3),
                options=BASE_OPTIONS
            ),
            1.53
        )

    def test_cast_array_to_array(self):
        self.assertEqual(
            cast_to_array(
                [1, 2, None, 4],
                ArrayType(ByteType()),
                ArrayType(StringType()),
                options=BASE_OPTIONS
            ),
            ['1', '2', None, '4']
        )

    def test_cast_map_to_map(self):
        self.assertEqual(
            cast_to_map(
                {1: "1", 2: "2"},
                MapType(ByteType(), StringType()),
                MapType(StringType(), FloatType()),
                options=BASE_OPTIONS
            ),
            {'1': 1.0, '2': 2.0}
        )

    def test_cast_to_struct(self):
        self.assertEqual(
            cast_to_struct(
                Row(character='Alice', day='28', month='8', year='2019'),
                from_type=StructType(fields=[
                    StructField("character", StringType()),
                    StructField("day", StringType()),
                    StructField("month", StringType()),
                    StructField("year", StringType()),
                ]),
                to_type=StructType(fields=[
                    StructField("character", StringType()),
                    StructField("day", IntegerType()),
                    StructField("month", IntegerType()),
                    StructField("year", IntegerType()),
                ]),
                options=BASE_OPTIONS
            ),
            Row(character='Alice', day=28, month=8, year=2019),
        )
