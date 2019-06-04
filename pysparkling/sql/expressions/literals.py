from pysparkling.sql.expressions.expressions import Expression


class Literal(Expression):
    def __init__(self, value):
        self.value = value

    def eval(self, row):
        return self.value

    def __str__(self):
        return str(self.value)
