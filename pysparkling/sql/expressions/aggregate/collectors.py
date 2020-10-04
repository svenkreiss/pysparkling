from pysparkling.sql.expressions.aggregate.aggregations import Aggregation


class CollectList(Aggregation):
    def __init__(self, column):
        super(CollectList, self).__init__(column)
        self.column = column
        self.items = []

    def merge(self, row, schema):
        self.items.append(self.column.eval(row, schema))

    def mergeStats(self, other, schema):
        self.items += other.items

    def eval(self, row, schema):
        return self.items

    def __str__(self):
        return "collect_list({0})".format(self.column)


class CollectSet(Aggregation):
    def __init__(self, column):
        super(CollectSet, self).__init__(column)
        self.column = column
        self.items = set()

    def merge(self, row, schema):
        self.items.add(self.column.eval(row, schema))

    def mergeStats(self, other, schema):
        self.items |= other.items

    def eval(self, row, schema):
        return list(self.items)

    def __str__(self):
        return "collect_set({0})".format(self.column)


class SumDistinct(Aggregation):
    def __init__(self, column):
        super(SumDistinct, self).__init__(column)
        self.column = column
        self.items = set()

    def merge(self, row, schema):
        self.items.add(self.column.eval(row, schema))

    def mergeStats(self, other, schema):
        self.items |= other.items

    def eval(self, row, schema):
        return sum(self.items)

    def __str__(self):
        return "sum_distinct({0})".format(self.column)


class First(Aggregation):
    _sentinel = object()

    def __init__(self, column, ignore_nulls):
        super(First, self).__init__(column)
        self.column = column
        self.value = self._sentinel
        self.ignore_nulls = ignore_nulls

    def merge(self, row, schema):
        if self.value is First._sentinel or (self.ignore_nulls and self.value is None):
            self.value = self.column.eval(row, schema)

    def mergeStats(self, other, schema):
        if self.value is First._sentinel or (self.ignore_nulls and self.value is None):
            self.value = other.value

    def eval(self, row, schema):
        return self.value if self.value is not First._sentinel else None

    def __str__(self):
        return "first({0}, {1})".format(self.column, str(self.ignore_nulls).lower())
