import sys
from unittest import TestCase

from pysparkling.sql.ast.ast_to_python import convert_tree
from pysparkling.sql.ast.parser import ast_parser
from pysparkling.sql.ast.utils import print_tree


class TestParser(TestCase):
    def test_where(self):
        parser = ast_parser("doesItWorks = 'In progress!'")
        tree = parser.booleanExpression()
        print_tree(tree)
        sys.stdin.flush()
        col = convert_tree(tree)
        print(col)
        self.assertEqual(str(col), "(doesItWorks = In progress!)")
