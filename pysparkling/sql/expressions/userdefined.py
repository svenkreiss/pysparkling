from .expressions import Expression


class UserDefinedFunction(Expression):
    def __init__(self, f, return_type, *exprs):
        super().__init__()
        self.f = f
        self.return_type = return_type
        self.exprs = exprs

    def eval(self, row, schema):
        return self.f(*(expr.eval(row, schema) for expr in self.exprs))

    def __str__(self):
        arguments = ', '.join(str(arg) for arg in self.args())
        return f"{self.f.__name__}({arguments})"

    def args(self):
        return self.exprs

    def data_type(self, schema):
        return self.return_type


__all__ = ["UserDefinedFunction"]
