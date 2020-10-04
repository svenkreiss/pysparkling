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


class ArrayMax(UnaryExpression):
    def eval(self, row, schema):
        return max(self.column.eval(row, schema))

    def __str__(self):
        return "array_max({0})".format(self.column)


class Slice(Expression):
    def __init__(self, x, start, length):
        self.x = x
        self.start = start
        self.length = length
        super(Slice, self).__init__(x)

    def eval(self, row, schema):
        return self.x.eval(row, schema)[self.start, self.start + self.length]

    def __str__(self):
        return "slice({0}, {1}, {2})".format(self.x, self.start, self.length)


class ArrayRepeat(Expression):
    def __init__(self, col, count):
        super(ArrayRepeat, self).__init__(col)
        self.col = col
        self.count = count

    def eval(self, row, schema):
        value = self.col.eval(row, schema)
        return [value for _ in range(self.count)]

    def __str__(self):
        return "array_repeat({0}, {1})".format(self.col, self.count)


class Sequence(Expression):
    def __init__(self, start, stop, step):
        super(Sequence, self).__init__(start, stop, step)
        self.start = start
        self.stop = stop
        self.step = step

    def eval(self, row, schema):
        start_value = self.start.eval(row, schema)
        stop_value = self.stop.eval(row, schema)
        if self.step is not None:
            step_value = self.step.eval(row, schema)
            if ((step_value < stop_value and step_value <= 0) or
                    (step_value > stop_value and step_value >= 0)):
                raise Exception(
                    "requirement failed: Illegal sequence boundaries: "
                    "{0} to {1} by {2}".format(
                        start_value,
                        stop_value,
                        step_value
                    )
                )
        else:
            step_value = 1 if start_value < stop_value else -1

        return list(range(start_value, stop_value, step_value))

    def __str__(self):
        return "array_join({0}, {1}{2})".format(
            self.start,
            self.stop,
            # Spark use the same logic of not displaying step
            # if it is None, even if it was explicitly set
            ", {0}".format(self.step) if self.step is not None else ""
        )


class ArrayJoin(Expression):
    def __init__(self, column, delimiter, nullReplacement):
        super(ArrayJoin, self).__init__(column)
        self.column = column
        self.delimiter = delimiter
        self.nullReplacement = nullReplacement

    def eval(self, row, schema):
        column_eval = self.column.eval(row, schema)
        return self.delimiter.join(
            value if value is not None else self.nullReplacement for value in column_eval
        )

    def __str__(self):
        return "array_join({0}, {1}{2})".format(
            self.column,
            self.delimiter,
            # Spark use the same logic of not displaying nullReplacement
            # if it is None, even if it was explicitly set
            ", {0}".format(self.nullReplacement) if self.nullReplacement is not None else ""
        )


class SortArray(Expression):
    def __init__(self, col, asc):
        super(SortArray, self).__init__(col)
        self.col = col
        self.asc = asc

    def eval(self, row, schema):
        return sorted(self.col.eval(row, schema), reverse=not self.asc)

    def __str__(self):
        return "sort_array({0}, {1})".format(
            self.col,
            self.asc
        )


class ArraysZip(Expression):
    def __init__(self, cols):
        super(ArraysZip, self).__init__(*cols)
        self.cols = cols

    def eval(self, row, schema):
        return [
            list(combination)
            for combination in zip(
                *(c.eval(row, schema) for c in self.cols)
            )
        ]

    def __str__(self):
        return "arrays_zip({0})".format(", ".join(self.cols))


class Flatten(UnaryExpression):
    def eval(self, row, schema):
        return [
            value
            for array in self.column.eval(row, schema)
            for value in array
        ]

    def __str__(self):
        return "flatten({0})".format(self.column)


class ArrayPosition(Expression):
    def __init__(self, col, value):
        super(ArrayPosition, self).__init__(col)
        self.col = col
        self.value = value

    def eval(self, row, schema):
        if self.value is None:
            return None
        col_eval = self.col.eval(row, schema)
        if col_eval is None:
            return None

        return col_eval.find(self.value) + 1

    def __str__(self):
        return "array_position({0}, {1})".format(self.col, self.value)


class ElementAt(Expression):
    def __init__(self, col, extraction):
        super(ElementAt, self).__init__(col)
        self.col = col
        self.extraction = extraction

    def eval(self, row, schema):
        col_eval = self.col.eval(row, schema)
        if isinstance(col_eval, list):
            return col_eval[self.extraction - 1]
        return col_eval.get(self.extraction)

    def __str__(self):
        return "element_at({0}, {1})".format(self.col, self.extraction)


class ArrayRemove(Expression):
    def __init__(self, col, element):
        super(ArrayRemove, self).__init__(col, element)
        self.col = col
        self.element = element

    def eval(self, row, schema):
        array = self.col.eval(row, schema)
        return [value for value in array if value != self.element]

    def __str__(self):
        return "array_remove({0}, {1})".format(self.col, self.element)


class ArrayDistinct(UnaryExpression):
    def eval(self, row, schema):
        return list(set(self.column.eval(row, schema)))

    def __str__(self):
        return "array_distinct({0})".format(self.column)


class ArrayIntersect(Expression):
    def __init__(self, col1, col2):
        super(ArrayIntersect, self).__init__(col1, col2)
        self.col1 = col1
        self.col2 = col2

    def eval(self, row, schema):
        return list(set(self.col1.eval(row, schema)) & set(self.col2.eval(row, schema)))

    def __str__(self):
        return "array_intersect({0}, {1})".format(self.col1, self.col2)

