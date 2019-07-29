from pyspark.sql.types import StructField, IntegerType, DataType

from pysparkling.sql.expressions.expressions import UnaryExpression


class Explode(UnaryExpression):
    def __init__(self, column):
        super().__init__(column)
        self.column = column

    @property
    def may_output_multiple_rows(self):
        return True

    def eval(self, row, schema):
        return [[value] for value in self.column.eval(row, schema)]

    def __str__(self):
        return "col"


class PosExplode(UnaryExpression):
    def eval(self, row, schema):
        values = self.column.eval(row, schema)
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
