from unittest import TestCase

from pysparkling.sql.ast.ast_to_python import parse_sql


class TestParser(TestCase):
    def test_where(self):
        col = parse_sql("doesItWorks = 'In progress!'", rule="booleanExpression", debug=True)
        self.assertEqual(str(col), "(doesItWorks = In progress!)")

    def test_struct(self):
        col = parse_sql("Struct('Alice', 2)", rule="primaryExpression", debug=True)
        self.assertEqual(str(col), "struct(Alice, 2)")
