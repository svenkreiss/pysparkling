from unittest import TestCase

from parameterized import parameterized
from parameterized.parameterized import default_name_func

from pysparkling import Row
from pysparkling.sql.ast.ast_to_python import parse_expression
from pysparkling.sql.types import StructType

ROW = Row()
SCHEMA = StructType()


def format_test_name(func, num, p):
    base_name = default_name_func(func, num, p)
    if len(p.args) > 1 and isinstance(p.args[1], tuple) and isinstance(p.args[1][0], str):
        return base_name + "_" + parameterized.to_safe_name(p.args[1][0])
    return base_name


class TestOperations(TestCase):
    SCENARIOS = {
        '60=60': ('EQ', '(60 = 60)', True),
        '60=12': ('EQ', '(60 = 12)', False),
        '60==12': ('EQ2', '(60 = 12)', False),
        '12<>12': ('NEQ', '(NOT (12 = 12))', False),
        '60<>12': ('NEQ', '(NOT (60 = 12))', True),
        '60!=12': ('NEQ2', '(NOT (60 = 12))', True),
        '60<12': ('LT', '(60 < 12)', False),
        '60<=12': ('LTE', '(60 <= 12)', False),
        '60!>12': ('LTE2', '(60 <= 12)', False),
        '60>12': ('GT', '(60 > 12)', True),
        '60>=12': ('GTE', '(60 >= 12)', True),
        '60!<12': ('GTE2', '(60 >= 12)', True),
        '60+12': ('PLUS', '(60 + 12)', 72),
        '60-12': ('MINUS', '(60 - 12)', 48),
        '60*12': ('TIMES', '(60 * 12)', 720),
        # '60/12': ('DIVIDE', '(60 / 12)', None),
        '60%12': ('MODULO', '(60 % 12)', 0),
        # '60 div 12': ('DIV', '(60 DIV 12)', None),
        '6&3': ('BITWISE_AND', '(6 & 3)', 2),
        '6|3': ('BITWISE_OR', '(6 | 3)', 7),
        # '60||12': ('CONCAT', '(60 || 12)', None),
        '6^3': ('BITWISE_XOR', '(6 ^ 3)', 5),
        'true and false': ('LOGICAL_AND', '(true AND false)', False),
        'TRUE AND TRUE': ('LOGICAL_AND', '(true AND true)', True),
        'true AND null': ('LOGICAL_AND', '(true AND NULL)', None),
        'True or False': ('LOGICAL_OR', '(true OR false)', True),
        'false or false': ('LOGICAL_OR', '(false OR false)', False),
        'true or NULL': ('LOGICAL_OR', '(true OR NULL)', None),
    }

    @parameterized.expand(SCENARIOS.items(), name_func=format_test_name)
    def test_operations(self, string, expected):
        operator, expected_parsed, expected_result = expected
        actual_parsed = parse_expression(string, True)
        self.assertEqual(expected_parsed, str(actual_parsed))
        actual_result = actual_parsed.eval(Row(), SCHEMA)
        self.assertEqual(expected_result, actual_result)
