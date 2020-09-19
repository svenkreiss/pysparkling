from unittest import TestCase

import pytest
from numpy.testing._private.parameterized import parameterized

from pysparkling.sql.types import *

from pysparkling.sql.ast.ast_to_python import string_to_type


class TypeParsingTest(TestCase):
    SCENARIOS = {
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
        # todo: "interval": CalendarIntervalType(),
    }

    @parameterized.expand(SCENARIOS.items())
    def test_equal(self, string, data_type):
        self.assertEqual(string_to_type(string), data_type)
