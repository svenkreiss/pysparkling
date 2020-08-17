from pysparkling.sql.expressions.expressions import Expression, UnaryExpression, \
    NullSafeBinaryOperation


class Negate(UnaryExpression):
    def eval(self, row, schema):
        return not self.column.eval(row, schema)

    def __str__(self):
        return "(- {0})".format(self.column)


class Add(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return value1 + value2

    def __str__(self):
        return "({0} + {1})".format(self.arg1, self.arg2)


class Minus(NullSafeBinaryOperation):
    def unsafe_operation(self, value1, value2):
        return value1 - value2

    def __str__(self):
        return "({0} - {1})".format(self.arg1, self.arg2)
