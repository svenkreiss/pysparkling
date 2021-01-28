from pysparkling.sql.expressions.expressions import BinaryOperation, Expression, UnaryExpression
from pysparkling.sql.utils import AnalysisException


class ArraysOverlap(BinaryOperation):
    pretty_name = "array_overlap"

    def eval(self, row, schema):
        set1 = set(self.arg1.eval(row, schema))
        set2 = set(self.arg2.eval(row, schema))
        overlap = set1 & set2
        if len(overlap) > 1 or (len(overlap) == 1 and None not in overlap):
            return True
        if set1 and set2 and (None in set1 or None in set2):
            return None
        return False


class ArrayContains(Expression):
    pretty_name = "array_contains"

    def __init__(self, array, value):
        self.array = array
        self.value = value.get_literal_value()
        super(ArrayContains, self).__init__(array)

    def eval(self, row, schema):
        array_eval = self.array.eval(row, schema)
        if array_eval is None:
            return None
        return self.value in array_eval

    def args(self):
        return (
            self.array,
            self.value
        )


class ArrayColumn(Expression):
    pretty_name = "array"

    def __init__(self, columns):
        super(ArrayColumn, self).__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        return [col.eval(row, schema) for col in self.columns]

    def args(self):
        return self.columns


class MapColumn(Expression):
    pretty_name = "map"

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

    def args(self):
        return self.columns


class MapFromArraysColumn(Expression):
    pretty_name = "map_from_arrays"

    def __init__(self, keys, values):
        super(MapFromArraysColumn, self).__init__(keys, values)
        self.keys = keys
        self.values = values

    def eval(self, row, schema):
        return dict(
            zip(self.keys.eval(row, schema), self.values.eval(row, schema))
        )

    def args(self):
        return (
            self.keys,
            self.values
        )


class Size(UnaryExpression):
    pretty_name = "size"

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


class ArraySort(UnaryExpression):
    pretty_name = "array_sort"

    def eval(self, row, schema):
        return sorted(self.column.eval(row, schema))


class ArrayMin(UnaryExpression):
    pretty_name = "array_min"

    def eval(self, row, schema):
        return min(self.column.eval(row, schema))


class ArrayMax(UnaryExpression):
    pretty_name = "array_max"

    def eval(self, row, schema):
        return max(self.column.eval(row, schema))


class Slice(Expression):
    pretty_name = "slice"

    def __init__(self, column, start, length):
        self.column = column
        self.start = start.get_literal_value()
        self.length = length.get_literal_value()
        super(Slice, self).__init__(column)

    def eval(self, row, schema):
        return self.column.eval(row, schema)[self.start, self.start + self.length]

    def args(self):
        return (
            self.column,
            self.start,
            self.length
        )


class ArrayRepeat(Expression):
    pretty_name = "array_repeat"

    def __init__(self, col, count):
        super(ArrayRepeat, self).__init__(col)
        self.col = col
        self.count = count.get_literal_value()

    def eval(self, row, schema):
        value = self.col.eval(row, schema)
        return [value for _ in range(self.count)]

    def args(self):
        return (
            self.col,
            self.count
        )


class Sequence(Expression):
    pretty_name = "array_join"

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
            if (step_value < stop_value and step_value <= 0) \
               or (step_value > stop_value and step_value >= 0):
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

    def args(self):
        if self.step is None:
            # Spark use the same logic of not displaying step
            # if it is None, even if it was explicitly set
            return (
                self.start,
                self.stop
            )
        return (
            self.start,
            self.stop,
            self.step
        )


class ArrayJoin(Expression):
    pretty_name = "array_join"

    def __init__(self, column, delimiter, nullReplacement):
        super(ArrayJoin, self).__init__(column)
        self.column = column
        self.delimiter = delimiter.get_literal_value()
        self.nullReplacement = nullReplacement.get_literal_value()

    def eval(self, row, schema):
        column_eval = self.column.eval(row, schema)
        return self.delimiter.join(
            value if value is not None else self.nullReplacement for value in column_eval
        )

    def args(self):
        if self.nullReplacement is None:
            # Spark use the same logic of not displaying nullReplacement
            # if it is None, even if it was explicitly set
            return (
                self.column,
                self.delimiter
            )
        return (
            self.column,
            self.delimiter,
            self.nullReplacement
        )


class SortArray(Expression):
    pretty_name = "sort_array"

    def __init__(self, col, asc):
        super(SortArray, self).__init__(col)
        self.col = col
        self.asc = asc.get_literal_value()

    def eval(self, row, schema):
        return sorted(self.col.eval(row, schema), reverse=not self.asc)

    def args(self):
        return (
            self.col,
            self.asc
        )


class ArraysZip(Expression):
    pretty_name = "arrays_zip"

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

    def args(self):
        return self.cols


class Flatten(UnaryExpression):
    pretty_name = "flatten"

    def eval(self, row, schema):
        return [
            value
            for array in self.column.eval(row, schema)
            for value in array
        ]


class ArrayPosition(Expression):
    pretty_name = "array_position"

    def __init__(self, col, value):
        super(ArrayPosition, self).__init__(col)
        self.col = col
        self.value = value.get_literal_value()

    def eval(self, row, schema):
        if self.value is None:
            return None
        col_eval = self.col.eval(row, schema)
        if col_eval is None:
            return None

        return col_eval.find(self.value) + 1

    def args(self):
        return (
            self.col,
            self.value
        )


class ElementAt(Expression):
    pretty_name = "element_at"

    def __init__(self, col, extraction):
        super(ElementAt, self).__init__(col)
        self.col = col
        self.extraction = extraction.get_literal_value()

    def eval(self, row, schema):
        col_eval = self.col.eval(row, schema)
        if isinstance(col_eval, list):
            return col_eval[self.extraction - 1]
        return col_eval.get(self.extraction)

    def args(self):
        return (
            self.col,
            self.extraction
        )


class ArrayRemove(Expression):
    pretty_name = "array_remove"

    def __init__(self, col, element):
        super(ArrayRemove, self).__init__(col, element)
        self.col = col
        self.element = element.get_literal_value()

    def eval(self, row, schema):
        array = self.col.eval(row, schema)
        return [value for value in array if value != self.element]

    def args(self):
        return (
            self.col,
            self.element
        )


class ArrayDistinct(UnaryExpression):
    pretty_name = "array_distinct"

    def eval(self, row, schema):
        return list(set(self.column.eval(row, schema)))


class ArrayIntersect(BinaryOperation):
    pretty_name = "array_intersect"

    def eval(self, row, schema):
        return list(set(self.arg1.eval(row, schema)) & set(self.arg2.eval(row, schema)))


class ArrayUnion(BinaryOperation):
    pretty_name = "array_union"

    def eval(self, row, schema):
        return list(set(self.arg1.eval(row, schema)) | set(self.arg2.eval(row, schema)))


class ArrayExcept(BinaryOperation):
    pretty_name = "array_except"

    def eval(self, row, schema):
        return list(set(self.arg1.eval(row, schema)) - set(self.arg2.eval(row, schema)))


__all__ = [
    "ArraysZip", "ArrayRepeat", "Flatten", "ArrayMax", "ArrayMin", "SortArray", "Size",
    "ArrayExcept", "ArrayUnion", "ArrayIntersect", "ArrayDistinct", "ArrayRemove", "ArraySort",
    "ElementAt", "ArrayPosition", "ArrayJoin", "ArraysOverlap", "ArrayContains",
    "MapFromArraysColumn", "MapColumn", "ArrayColumn", "Slice", "Sequence"
]
