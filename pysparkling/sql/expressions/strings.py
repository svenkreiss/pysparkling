from pysparkling.sql.expressions.expressions import Expression


class StringTrim(Expression):
    def __init__(self, column):
        super().__init__(column)
        self.column = column

    def eval(self, row, schema):
        return self.column.eval(row, schema).strip()

    def __str__(self):
        return "trim({0})".format(self.column)
