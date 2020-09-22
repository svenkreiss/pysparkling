from unittest import TestCase

from pysparkling.sql.ast.ast_to_python import parse_sql, SqlParsingError


class TestParser(TestCase):
    def test_where(self):
        col = parse_sql("doesItWorks = 'In progress!'", rule="booleanExpression")
        self.assertEqual("(doesItWorks = In progress!)", str(col))

    def test_named_expression_no_alias(self):
        col = parse_sql("1 + 2", rule="singleExpression")
        self.assertEqual("(1 + 2)", str(col))

    def test_named_expression_implicit_alias(self):
        col = parse_sql("(1 + 2) sum", rule="singleExpression")
        self.assertEqual("sum", str(col))

    def test_named_expression_explicit_alias(self):
        col = parse_sql("1 + 2 as sum", rule="singleExpression")
        self.assertEqual("sum", str(col))

    def test_named_expression_bad_alias(self):
        with self.assertRaises(SqlParsingError) as ctx:
            parse_sql("1 + 2 as invalid-alias", rule="singleExpression")
        self.assertEqual(
            'Possibly unquoted identifier invalid-alias detected. '
            'Please consider quoting it with back-quotes as `invalid-alias`',
            str(ctx.exception)
        )

    def test_struct(self):
        col = parse_sql("Struct('Alice', 2)", rule="primaryExpression")
        self.assertEqual("struct(Alice, 2)", str(col))
