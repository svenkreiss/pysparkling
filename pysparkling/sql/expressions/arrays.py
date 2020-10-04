from pysparkling.sql.expressions.expressions import Expression, UnaryExpression
from pysparkling.sql.utils import AnalysisException


class ArraysOverlap(Expression):
    def __init__(self, array1, array2):
        super(ArraysOverlap, self).__init__(array1, array2)
        self.array1 = array1
        self.array2 = array2

    def eval(self, row, schema):
        set1 = set(self.array1.eval(row, schema))
        set2 = set(self.array2.eval(row, schema))
        overlap = set1 & set2
        if len(overlap) > 1 or (len(overlap) == 1 and None not in overlap):
            return True
        if set1 and set2 and (None in set1 or None in set2):
            return None
        return False

    def __str__(self):
        return "array_overlap({0}, {1})".format(self.array1, self.array2)


class ArrayContains(Expression):
    def __init__(self, array, value):
        self.array = array
        self.value = value  # not a column
        super(ArrayContains, self).__init__(array)

    def eval(self, row, schema):
        array_eval = self.array.eval(row, schema)
        if array_eval is None:
            return None
        return self.value in array_eval

    def __str__(self):
        return "array_contains({0}, {1})".format(self.array, self.value)


class ArrayColumn(Expression):
    def __init__(self, columns):
        super(ArrayColumn, self).__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        return [col.eval(row, schema) for col in self.columns]

    def __str__(self):
        return "array({0})".format(", ".join(str(col) for col in self.columns))


class MapColumn(Expression):
    def __init__(self, columns):
        super(MapColumn, self).__init__(columns)
        self.columns = columns
        self.keys = columns[::2]
        self.values = columns[1::2]

    def eval(self, row, schema):
        return dict(
            (key.eval(row, schema), value.eval(row, schema))
            for key, value in zip(self.keys, self.values)
        )

    def __str__(self):
        return "map({0})".format(", ".join(str(col) for col in self.columns))


class MapFromArraysColumn(Expression):
    def __init__(self, keys, values):
        super(MapFromArraysColumn, self).__init__(keys, values)
        self.keys = keys
        self.values = values

    def eval(self, row, schema):
        return dict(
            zip(self.keys.eval(row, schema), self.values.eval(row, schema))
        )

    def __str__(self):
        return "map_from_arrays({0}, {1})".format(
            self.keys,
            self.values
        )


class Size(UnaryExpression):
    def eval(self, row, schema):
        column_value = self.column.eval(row, schema)
        if isinstance(column_value, (list, dict)):
            return len(column_value)
        raise AnalysisException(
            "{0} value should be an array or a map, got {1}".format(
                self.column,
                type(column_value)
            )
        )

    def __str__(self):
        return "size({0})".format(self.column)


class ArraySort(UnaryExpression):
    def eval(self, row, schema):
        return sorted(self.column.eval(row, schema))

    def __str__(self):
        return "array_sort({0})".format(self.column)


class ArrayMin(UnaryExpression):
    def eval(self, row, schema):
        return min(self.column.eval(row, schema))

    def __str__(self):
        return "array_min({0})".format(self.column)

