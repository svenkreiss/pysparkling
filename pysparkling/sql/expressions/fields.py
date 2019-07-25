from pysparkling.sql.column import Column
from pysparkling.sql.expressions.expressions import Expression


class FieldAsExpression(Expression):
    def __init__(self, field):
        super().__init__()
        self.field = field

    def eval(self, row, schema):
        return row[Column(self.field).find_position_in_schema(schema)]

    def __str__(self):
        return self.field.name

