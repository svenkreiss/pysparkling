from pysparkling.sql.expressions.aggregate.aggregations import Aggregation


class CollectList(Aggregation):
    pretty_name = "collect_list"

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

    def args(self):
        return (self.column,)


class CollectSet(Aggregation):
    pretty_name = "collect_set"

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

    def args(self):
        return (self.column,)


class SumDistinct(Aggregation):
    pretty_name = "sum_distinct"

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

    def args(self):
        return (self.column,)


class First(Aggregation):
    pretty_name = "first"
    _sentinel = object()

    def __init__(self, column, ignore_nulls):
        super(First, self).__init__(column)
        self.column = column
        self.value = self._sentinel
        self.ignore_nulls = ignore_nulls.get_literal_value()

    def merge(self, row, schema):
        if self.value is First._sentinel or (self.ignore_nulls and self.value is None):
            self.value = self.column.eval(row, schema)

    def mergeStats(self, other, schema):
        if self.value is First._sentinel or (self.ignore_nulls and self.value is None):
            self.value = other.value

    def eval(self, row, schema):
        return self.value if self.value is not First._sentinel else None

    def args(self):
        return (
            self.column,
            str(self.ignore_nulls).lower()
        )


class Last(Aggregation):
    pretty_name = "last"
    _sentinel = object()

    def __init__(self, column, ignore_nulls):
        super(Last, self).__init__(column)
        self.column = column
        self.value = None
        self.ignore_nulls = ignore_nulls.get_literal_value()

    def merge(self, row, schema):
        new_value = self.column.eval(row, schema)
        if not (self.ignore_nulls and new_value is None):
            self.value = new_value

    def mergeStats(self, other, schema):
        if not (self.ignore_nulls and other.value is None):
            self.value = other.value

    def eval(self, row, schema):
        return self.value

    def args(self):
        return (
            self.column,
            str(self.ignore_nulls).lower()
        )


class CountDistinct(Aggregation):
    pretty_name = "count"

    def __init__(self, columns):
        super(CountDistinct, self).__init__(columns)
        self.columns = columns
        self.items = set()

    def merge(self, row, schema):
        self.items.add(tuple(
            col.eval(row, schema) for col in self.columns
        ))

    def mergeStats(self, other, schema):
        self.items += other.items

    def eval(self, row, schema):
        return len(self.items)

    def args(self):
        return "DISTINCT {0}".format(",".join(self.columns))


class ApproxCountDistinct(Aggregation):
    pretty_name = "approx_count_distinct"

    def __init__(self, column):
        super(ApproxCountDistinct, self).__init__(column)
        self.column = column
        self.items = set()

    def merge(self, row, schema):
        self.items.add(self.column.eval(row, schema))

    def mergeStats(self, other, schema):
        self.items += other.items

    def eval(self, row, schema):
        return len(self.items)

    def args(self):
        return (self.column,)


__all__ = [
    "SumDistinct", "ApproxCountDistinct", "CollectList", "CollectSet",
    "First", "CountDistinct", "Last"
]
