from pysparkling.sql.expressions.expressions import Expression, UnaryExpression


class ArraysOverlap(Expression):
    def __init__(self, array1, array2):
        super().__init__(array1, array2)
        self.array1 = array1
        self.array2 = array2

    def eval(self, row):
        set1 = set(self.array1.eval(row))
        set2 = set(self.array2.eval(row))
        overlap = set1 & set2
        if len(overlap) > 1 or (len(overlap) == 1 and None not in overlap):
            return True
        if len(set1) > 0 and len(set2) > 0 and (None in set1 or None in set2):
            return None
        return False

    def __str__(self):
        return "array_overlap({0}, {1})".format(self.array1, self.array2)


class ArrayContains(Expression):
    def __init__(self, array, value):
        self.array = array
        self.value = value  # not a column
        super().__init__(array)

    def eval(self, row):
        array_eval = self.array.eval(row)
        if array_eval is None:
            return None
        return self.value in array_eval

    def __str__(self):
        return "array_contains({0}, {1))".format(self.array, self.value)


class ArrayColumn(Expression):
    def __init__(self, columns):
        super().__init__(columns)
        self.columns = columns

    def eval(self, row):
        return [col.eval(row) for col in self.columns]

    def __str__(self):
        return "array({0})".format(", ".join(str(col) for col in self.columns))


class MapColumn(Expression):
    def __init__(self, *columns):
        super().__init__(columns)
        self.columns = columns
        self.keys = columns[::2]
        self.values = columns[1::2]

    def eval(self, row):
        return dict((key.eval(row), value.eval(row)) for key, value in zip(self.keys, self.values))

    def __str__(self):
        return "map({0})".format(", ".join(str(col) for col in self.columns))


class MapFromArraysColumn(Expression):
    def __init__(self, keys, values):
        super().__init__(keys, values)
        self.keys = keys
        self.values = values

    def eval(self, row):
        return dict(zip(self.keys.eval(row), self.values.eval(row)))

    def __str__(self):
        return "map_from_arrays({0}, {1})".format(
            self.keys,
            self.values
        )


class Size(UnaryExpression):
    def eval(self, row):
        column_value = self.column.eval(row)
        if isinstance(column_value, (list, set, dict)):
            return len(column_value)
        raise Expression("{0} value should be a list, set or a dict, got {1}".format(self.column, type(column_value)))

    def __str__(self):
        return "size({0})".format(self.column)


class ArraySort(UnaryExpression):
    def eval(self, row):
        return sorted(self.column.eval(row))

    def __str__(self):
        return "array_sort({0})".format(self.column)


class ArrayMin(UnaryExpression):
    def eval(self, row):
        return min(self.column.eval(row))

    def __str__(self):
        return "array_min({0})".format(self.column)


class ArrayMax(UnaryExpression):
    def eval(self, row):
        return max(self.column.eval(row))

    def __str__(self):
        return "array_max({0})".format(self.column)


class Slice(Expression):
    def __init__(self, x, start, length):
        self.x = x
        self.start = start
        self.length = length
        super().__init__(x)

    def eval(self, row):
        return self.x.eval(row)[self.start, self.start + self.length]

    def __str__(self):
        return "slice({0}, {1}, {2})".format(self.x, self.start, self.length)


class ArrayJoin(Expression):
    def __init__(self, column, delimiter, nullReplacement):
        super().__init__(column)
        self.column = column
        self.delimiter = delimiter
        self.nullReplacement = nullReplacement

    def eval(self, row):
        column_eval = self.column.eval(row)
        return self.delimiter.join(
            value if value is not None else self.nullReplacement for value in column_eval
        )

    def __str__(self):
        return "array_join({0}, {1}{2})".format(
            self.column,
            self.delimiter,
            # Spark use the same logic of not display nullReplacement
            # if it is None, even if it was explicitly set
            " {0}".format(self.nullReplacement) if self.nullReplacement is not None else ""
        )


class ArrayPosition(Expression):
    def __init__(self, col, value):
        super().__init__(col)
        self.col = col
        self.value = value

    def eval(self, row):
        if self.value is None:
            return None
        col_eval = self.col.eval(row)
        if col_eval is None:
            return None

        return col_eval.find(self.value) + 1

    def __str__(self):
        return "array_position({0}, {1})".format(self.col, self.value)


class ElementAt(Expression):
    def __init__(self, col, extraction):
        super().__init__(col)
        self.col = col
        self.extraction = extraction

    def eval(self, row):
        col_eval = self.col.eval(row)
        if isinstance(col_eval, list):
            return col_eval[self.extraction - 1]
        return col_eval.get(self.extraction)

    def __str__(self):
        return "element_at({0}, {1})".format(self.col, self.extraction)


class ArrayRemove(Expression):
    def __init__(self, col, element):
        super().__init__(col, element)
        self.col = col
        self.element = element

    def eval(self, row):
        array = self.col.eval(row)
        return [value for value in array if value != self.element]

    def __str__(self):
        return "array_remove({0}, {1})".format(self.col, self.element)


class ArrayDistinct(UnaryExpression):
    def eval(self, row):
        return list(set(self.column.eval(row)))

    def __str__(self):
        return "array_distinct({0})".format(self.column)


class ArrayIntersect(Expression):
    def __init__(self, col1, col2):
        super().__init__(col1, col2)
        self.col1 = col1
        self.col2 = col2

    def eval(self, row):
        return list(set(self.col1.eval(row)) & set(self.col2.eval(row)))

    def __str__(self):
        return "array_intersect({0}, {1})".format(self.col1, self.col2)


class ArrayUnion(Expression):
    def __init__(self, col1, col2):
        super().__init__(col1, col2)
        self.col1 = col1
        self.col2 = col2

    def eval(self, row):
        return list(set(self.col1.eval(row)) | set(self.col2.eval(row)))

    def __str__(self):
        return "array_union({0}, {1})".format(self.col1, self.col2)


class ArrayExcept(Expression):
    def __init__(self, col1, col2):
        super().__init__(col1, col2)
        self.col1 = col1
        self.col2 = col2

    def eval(self, row):
        return list(set(self.col1.eval(row)) - set(self.col2.eval(row)))

    def __str__(self):
        return "array_except({0}, {1})".format(self.col1, self.col2)


