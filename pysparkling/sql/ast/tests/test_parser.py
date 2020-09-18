import sys
from unittest import TestCase

import antlr4

from pysparkling.sql.ast.ast_to_python import convert_tree
from pysparkling.sql.ast.parser import build_ast
from pysparkling.sql.ast.utils import print_tree


class TestParser(TestCase):
    def test_where(self):
        parser = build_ast(antlr4.FileStream("test-where.sql"))
        tree = parser.booleanExpression()
        print_tree(tree)
        sys.stdin.flush()
        col = convert_tree(tree)
        print(col)
        self.assertEqual(str(col), '(doesItWorks = "In progress!")')
