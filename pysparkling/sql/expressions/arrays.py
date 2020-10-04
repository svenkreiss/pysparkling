from pysparkling.sql.expressions.expressions import Expression


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


class ArrayColumn(Expression):
    def __init__(self, columns):
        super(ArrayColumn, self).__init__(columns)
        self.columns = columns

    def eval(self, row, schema):
        return [col.eval(row, schema) for col in self.columns]

    def __str__(self):
        return "array({0})".format(", ".join(str(col) for col in self.columns))


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
