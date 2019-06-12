from pysparkling.sql.expressions.expressions import Expression


class Explode(Expression):
    def __init__(self, column):
        super().__init__(column)
        self.column = column

    @property
    def may_output_multiple_rows(self):
        return True

    def eval(self, row):
        return list(sub_value for sub_value in self.column.eval(row))

    def __str__(self):
        return "col"
