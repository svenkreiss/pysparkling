from pysparkling.sql.expressions.expressions import Expression


class Literal(Expression):
    def __init__(self, value):
        super(Literal, self).__init__()
        self.value = value

    def eval(self, row, schema):
        return self.value

    def __str__(self):
        return str(self.value)


__all__ = ["Literal"]
