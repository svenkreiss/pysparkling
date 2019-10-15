from pysparkling.sql.expressions.expressions import Expression


class UserDefinedFunction(Expression):
    def __init__(self, f, return_type, *exprs):
        super().__init__()
        self.f = f
        self.return_type = return_type
        self.exprs = exprs

    def eval(self, row, schema):
        return self.f(*(expr.eval(row, schema) for expr in self.exprs))

    def __str__(self):
        return "{0}({1})".format(
            self.f.__name__,
            ", ".join(str(expr) for expr in self.exprs)
        )
