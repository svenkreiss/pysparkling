from pysparkling.sql.types import StructField, IntegerType, DataType

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


class ExplodeOuter(Explode):
    def eval(self, row, schema):
        values = self.column.eval(row, schema)
        if not values:
            return [[None]]
        return [[value] for value in values]

    def __str__(self):
        return "col"


class PosExplode(UnaryExpression):
    def eval(self, row, schema):
        values = self.column.eval(row, schema)
        if not values:
            return []
        return list(enumerate(values))

    def __str__(self):
        return "posexplode"

    @property
    def may_output_multiple_rows(self):
        return True

    @property
    def may_output_multiple_cols(self):
        return True

    def output_fields(self, schema):
        return [
            StructField("pos", IntegerType(), False),
            StructField("col", DataType(), False)
        ]


class PosExplodeOuter(PosExplode):
    def eval(self, row, schema):
        values = self.column.eval(row, schema)
        if not values:
            return [[None, None]]
        return list(enumerate(values))

    def __str__(self):
        return "posexplode_outer"


__all__ = ["PosExplodeOuter", "PosExplode", "ExplodeOuter", "Explode"]
