from ..types import BooleanType, ArrayType, NullType, MapType, IntegerType, StringType, StructType
from ..utils import AnalysisException
from .expressions import BinaryOperation, Expression, UnaryExpression


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

    def data_type(self, schema):
        return BooleanType()


class ArrayContains(Expression):
    pretty_name = "array_contains"

    def __init__(self, array, value):
        self.array = array
        self.value = value.get_literal_value()
        super().__init__(array)

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

    def data_type(self, schema):
        return BooleanType()


class ArrayColumn(Expression):
    pretty_name = "array"

    def __init__(self, *columns):
        super().__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        return [col.eval(row, schema) for col in self.columns]

    def args(self):
        return self.columns

    def data_type(self, schema):
        if not self.columns:
            return ArrayType(elementType=NullType)
        return ArrayType(elementType=self.columns[0].data_type(schema))


class MapColumn(Expression):
    pretty_name = "map"

    def __init__(self, *columns):
        super().__init__(columns)
        self.columns = columns
        if len(columns) % 2 != 0:
            raise AnalysisException(
                f"Cannot resolve '{self}' due to data type mismatch: "
                f"map expects a positive even number of arguments."
            )
        self.keys = columns[::2]
        self.values = columns[1::2]

    def eval(self, row, schema):
        return dict(
            (key.eval(row, schema), value.eval(row, schema))
            for key, value in zip(self.keys, self.values)
        )

    def args(self):
        return self.columns

    def data_type(self, schema):
        if not self.columns:
            return MapType(keyType=NullType, valueType=NullType)
        return MapType(
            keyType=self.keys[0].data_type(schema),
            valueType=self.values[0].data_type(schema),
        )


class MapFromArraysColumn(Expression):
    pretty_name = "map_from_arrays"

    def __init__(self, keys, values):
        super().__init__(keys, values)
        self.keys = keys
        self.values = values

    def eval(self, row, schema):
        keys = self.keys.eval(row, schema)
        values = self.values.eval(row, schema)
        if len(keys) != len(values):
            raise AnalysisException(
                f"Error in '{self}':  The key array and value array of MapData must have the same length."
            )
        return dict(zip(keys, values))

    def args(self):
        return (
            self.keys,
            self.values
        )

    def data_type(self, schema):
        if not isinstance(self.keys, Column) and not self.keys:
            return MapType(keyType=NullType, valueType=NullType)
        return MapType(
            keyType=self.keys[0].data_type(schema),
            valueType=self.values[0].data_type(schema),
        )


class Size(UnaryExpression):
    pretty_name = "size"

    def eval(self, row, schema):
        column_value = self.column.eval(row, schema)
        if isinstance(column_value, (list, dict)):
            return len(column_value)
        raise AnalysisException(
            f"{self.column} value should be an array or a map, got {type(column_value)}"
        )

    def data_type(self, schema):
        return IntegerType()


class ArraySort(UnaryExpression):
    pretty_name = "array_sort"

    def eval(self, row, schema):
        return sorted(self.column.eval(row, schema))

    def data_type(self, schema):
        return self.column.data_type(schema)


class ArrayMin(UnaryExpression):
    pretty_name = "array_min"

    def eval(self, row, schema):
        column_type = self.column.data_type(schema)
        column_value = self.column.eval(row, schema)
        if not column_type == ArrayType:
            raise AnalysisException(
                f"Cannot resolve '{self}' due to data type mismatch: argument 1 requires array type, "
                f"however, '{column_value}' is of {column_type} type."
            )
        return min(column_value)

    def data_type(self, schema):
        return self.column.data_type(schema).elementType()


class ArrayMax(UnaryExpression):
    pretty_name = "array_max"

    def eval(self, row, schema):
        column_type = self.column.data_type(schema)
        column_value = self.column.eval(row, schema)
        if not column_type == ArrayType:
            raise AnalysisException(
                f"Cannot resolve '{self}' due to data type mismatch: argument 1 requires array type, "
                f"however, '{column_value}' is of {column_type} type."
            )
        return max(column_value)

    def data_type(self, schema):
        return self.column.data_type(schema).elementType()


class Slice(Expression):
    pretty_name = "slice"

    def __init__(self, column, start, length):
        self.column = column
        self.start = start.get_literal_value()
        self.length = length.get_literal_value()
        super().__init__(column)

    def eval(self, row, schema):
        return self.column.eval(row, schema)[self.start, self.start + self.length]

    def args(self):
        return (
            self.column,
            self.start,
            self.length
        )

    def data_type(self, schema):
        return self.column.data_type(schema)


class ArrayRepeat(Expression):
    pretty_name = "array_repeat"

    def __init__(self, col, count):
        super().__init__(col)
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

    def data_type(self, schema):
        return ArrayType(self.col.data_type(schema))


class Sequence(Expression):
    pretty_name = "sequence"

    def __init__(self, start, stop, step):
        super().__init__(start, stop, step)
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
                    "requirement failed: Illegal sequence boundaries:"
                    f" {start_value} to {stop_value} by {step_value}"
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

    def data_type(self, schema):
        return ArrayType(self.start.data_type(schema))


class ArrayJoin(Expression):
    pretty_name = "array_join"

    def __init__(self, column, delimiter, nullReplacement):
        super().__init__(column)
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

    def data_type(self, schema):
        return StringType()


class SortArray(Expression):
    pretty_name = "sort_array"

    def __init__(self, col, asc):
        super().__init__(col)
        self.col = col
        self.asc = asc.get_literal_value()

    def eval(self, row, schema):
        return sorted(self.col.eval(row, schema), reverse=not self.asc)

    def args(self):
        return (
            self.col,
            self.asc
        )

    def data_type(self, schema):
        return self.col.data_type(schema)


class ArraysZip(Expression):
    pretty_name = "arrays_zip"

    def __init__(self, columns):
        super().__init__(*columns)
        self.columns = columns

    def eval(self, row, schema):
        return [
            {i: v for i, v in enumerate(combination)}
            for combination in zip(
                *(c.eval(row, schema) for c in self.columns)
            )
        ]

    def args(self):
        return self.columns

    def data_type(self, schema):
        return ArrayType(StructType([
            col.data_type(schema) for col in self.columns
        ]))


class Flatten(UnaryExpression):
    pretty_name = "flatten"

    def eval(self, row, schema):
        return [
            value
            for array in self.column.eval(row, schema)
            for value in array
        ]

    def data_type(self, schema):
        return self.column.data_type(schema).elementType


class ArrayPosition(Expression):
    pretty_name = "array_position"

    def __init__(self, col, value):
        super().__init__(col)
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

    def data_type(self, schema):
        return IntegerType()


class ElementAt(Expression):
    pretty_name = "element_at"

    def __init__(self, col, extraction):
        super().__init__(col)
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

    def data_type(self, schema):
        return self.col.data_type(schema).elementType


class ArrayRemove(Expression):
    pretty_name = "array_remove"

    def __init__(self, col, element):
        super().__init__(col, element)
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

    def data_type(self, schema):
        return self.col.data_type(schema)


class ArrayDistinct(UnaryExpression):
    pretty_name = "array_distinct"

    def eval(self, row, schema):
        return list(set(self.column.eval(row, schema)))

    def data_type(self, schema):
        return self.column.data_type(schema)


class ArrayIntersect(BinaryOperation):
    pretty_name = "array_intersect"

    def eval(self, row, schema):
        return list(set(self.arg1.eval(row, schema)) & set(self.arg2.eval(row, schema)))

    def data_type(self, schema):
        return self.arg1.data_type(schema)


class ArrayUnion(BinaryOperation):
    pretty_name = "array_union"

    def eval(self, row, schema):
        return list(set(self.arg1.eval(row, schema)) | set(self.arg2.eval(row, schema)))

    def data_type(self, schema):
        return self.arg1.data_type(schema)


class ArrayExcept(BinaryOperation):
    pretty_name = "array_except"

    def eval(self, row, schema):
        return list(set(self.arg1.eval(row, schema)) - set(self.arg2.eval(row, schema)))

    def data_type(self, schema):
        return self.arg1.data_type(schema)


__all__ = [
    "ArraysZip", "ArrayRepeat", "Flatten", "ArrayMax", "ArrayMin", "SortArray", "Size",
    "ArrayExcept", "ArrayUnion", "ArrayIntersect", "ArrayDistinct", "ArrayRemove", "ArraySort",
    "ElementAt", "ArrayPosition", "ArrayJoin", "ArraysOverlap", "ArrayContains",
    "MapFromArraysColumn", "MapColumn", "ArrayColumn", "Slice", "Sequence"
]
