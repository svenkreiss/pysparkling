from .expressions import Expression


class SortOrder(Expression):
    sort_order = None

    def __init__(self, column):
        super().__init__(column)
        self.column = column

    def eval(self, row, schema):
        return self.column.eval(row, schema)

    def __str__(self):
        return f"{self.column} {self.sort_order}"

    def args(self):
        return (self.column,)

    def data_type(self, schema):
        return self.column.data_type(schema)


class AscNullsFirst(SortOrder):
    sort_order = "ASC NULLS FIRST"


class AscNullsLast(SortOrder):
    sort_order = "ASC NULLS LAST"


class DescNullsFirst(SortOrder):
    sort_order = "DESCNULLS FIRST"


class DescNullsLast(SortOrder):
    sort_order = "DESC NULLS LAST"


Asc = AscNullsFirst
Desc = DescNullsLast
