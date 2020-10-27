from pysparkling.sql.expressions.expressions import Expression
from pysparkling.sql.utils import AnalysisException


class Literal(Expression):
    def __init__(self, value):
        super(Literal, self).__init__()
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
            raise AnalysisException("Value should not be a Column or an Expression, "
                                    "but got {0}: {1}".format(type(self), self))
        return self.value


__all__ = ["Literal"]
