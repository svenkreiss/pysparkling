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
