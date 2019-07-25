from pysparkling.sql.expressions.aggregate.aggregations import Aggregation


class CollectList(Aggregation):
    def __init__(self, column):
        super().__init__(column)
        self.column = column
        self.items = []

    def merge(self, row, schema):
        self.items.append(self.column.eval(row, schema))

    def mergeStats(self, other):
        self.items += other.items

    def eval(self, row, schema):
        return self.items

    def __str__(self):
        return "collect_list({0})".format(self.column)


class CollectSet(Aggregation):
    def __init__(self, column):
        super().__init__(column)
        self.column = column
        self.items = set()

    def merge(self, row, schema):
        self.items.add(self.column.eval(row, schema))

    def mergeStats(self, other):
        self.items |= other.items

    def eval(self, row, schema):
        return list(self.items)

    def __str__(self):
        return "collect_set({0})".format(self.column)


class SumDistinct(Aggregation):
    def __init__(self, column):
        super().__init__(column)
        self.column = column
        self.items = set()

    def merge(self, row, schema):
        self.items.add(self.column.eval(row, schema))

    def mergeStats(self, other):
        self.items |= other.items

    def eval(self, row, schema):
        return sum(self.items)

    def __str__(self):
        return "sum_distinct({0})".format(self.column)


class First(Aggregation):
    _sentinel = object()

    def __init__(self, column, ignore_nulls):
        super().__init__(column)
        self.column = column
        self.value = self._sentinel
        self.ignore_nulls = ignore_nulls

    def merge(self, row, schema):
        if self.value is First._sentinel or (self.ignore_nulls and self.value is None):
            self.value = self.column.eval(row, schema)

    def mergeStats(self, other):
        if self.value is First._sentinel or (self.ignore_nulls and self.value is None):
            self.value = other.value

    def eval(self, row, schema):
        return self.value if self.value is not First._sentinel else None

    def __str__(self):
        return "first({0}, {1})".format(self.column, str(self.ignore_nulls).lower())


class Last(Aggregation):
    _sentinel = object()

    def __init__(self, column, ignore_nulls):
        super().__init__(column)
        self.column = column
        self.value = None
        self.ignore_nulls = ignore_nulls

    def merge(self, row, schema):
        new_value = self.column.eval(row, schema)
        if not (self.ignore_nulls and new_value is None):
            self.value = new_value

    def mergeStats(self, other):
        if not (self.ignore_nulls and other.value is None):
            self.value = other.value

    def eval(self, row, schema):
        return self.value

    def __str__(self):
        return "last({0}, {1})".format(self.column, str(self.ignore_nulls).lower())


class CountDistinct(Aggregation):
    def __init__(self, columns):
        super().__init__(columns)
        self.columns = columns
        self.items = set()

    def merge(self, row, schema):
        self.items.add(tuple(
            col.eval(row, schema) for col in self.columns
        ))

    def mergeStats(self, other):
        self.items += other.items

    def eval(self, row, schema):
        return len(self.items)

    def __str__(self):
        return "count(DISTINCT {0})".format(",".join(self.columns))

