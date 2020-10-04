from pysparkling.sql.expressions.expressions import UnaryExpression


class Explode(UnaryExpression):
    def __init__(self, column):
        super(Explode, self).__init__(column)
        self.column = column

    @property
    def may_output_multiple_rows(self):
        return True

    def eval(self, row, schema):
        values = self.column.eval(row, schema)
        if not values:
            return []
        return [[value] for value in values]

    def __str__(self):
        return "col"
