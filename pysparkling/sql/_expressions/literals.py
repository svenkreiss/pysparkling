from . import Expression
from ..utils import AnalysisException


class Literal(Expression):
    def __init__(self, value):
        super().__init__()
        self.value = value

    def eval(self, row, schema):
        return self.value

    def __str__(self):
        if self.value is True:
            return "true"
        if self.value is False:
            return "false"
        if self.value is None:
            return "NULL"
        return str(self.value)

    def get_literal_value(self):
        if hasattr(self.value, "expr") or isinstance(self.value, Expression):
            raise AnalysisException("Value should not be a Column or an Expression,"
                                    f" but got {type(self)}: {self}")
        return self.value

    def args(self):
        return (self.value, )


__all__ = ["Literal"]
