from unittest import TestCase

from parameterized import parameterized
from parameterized.parameterized import default_name_func

from pysparkling import Row
from pysparkling.sql.ast.ast_to_python import parse_expression
from pysparkling.sql.types import StructType, IntegerType, StructField

ROW = Row(a=1, b=2, c=3)
SCHEMA = StructType([
    StructField("a", IntegerType()),
    StructField("b", IntegerType()),
])


def format_test_name(func, num, p):
    base_name = default_name_func(func, num, p)
    if len(p.args) > 1 and isinstance(p.args[1], tuple) and isinstance(p.args[1][0], str):
        return base_name + "_" + parameterized.to_safe_name(p.args[1][0])
    return base_name


class TestFunctions(TestCase):
    SCENARIOS = {
        'Least(-1,0,1)': ('least', 'least(-1, 0, 1)', -1),
        'GREATEST(-1,0,1)': ('greatest', 'greatest(-1, 0, 1)', 1),
        'shiftRight ( 42, 1 )': ('shiftRight', 'shiftright(42, 1)', 21),
        'ShiftLeft ( 42, 1 )': ('shiftLeft', 'shiftleft(42, 1)', 84),
        'concat_ws(/, a, b )': ('shiftLeft', 'shiftleft(42, 1)', 84),
        'instr(a, a)': ('shiftLeft', 'shiftleft(42, 1)', 84),
    }

    @parameterized.expand(SCENARIOS.items(), name_func=format_test_name)
    def test_functions(self, string, expected):
        operator, expected_parsed, expected_result = expected
        actual_parsed = parse_expression(string, True)
        self.assertEqual(expected_parsed, str(actual_parsed))
        actual_result = actual_parsed.eval(ROW, SCHEMA)
        self.assertEqual(expected_result, actual_result)
