from pysparkling.sql.column import Column
from pysparkling.sql.expressions.aggregate.aggregations import Aggregation
from pysparkling.sql.expressions.literals import Literal
from pysparkling.sql.expressions.mappers import StarOperator


class SimpleStatAggregation(Aggregation):
    def __init__(self, column):
        # Top level import would cause cyclic dependencies
        # pylint: disable=import-outside-toplevel
        from pysparkling.stat_counter import ColumnStatHelper
        super(SimpleStatAggregation, self).__init__(column)
        self.column = column
        self.stat_helper = ColumnStatHelper(column)

    def merge(self, row, schema):
        self.stat_helper.merge(row, schema)

    def mergeStats(self, other, schema):
        self.stat_helper.mergeStats(other.stat_helper)

    def eval(self, row, schema):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError


class Count(SimpleStatAggregation):
    def __init__(self, column):
        # Top level import would cause cyclic dependencies
        # pylint: disable=import-outside-toplevel
        from pysparkling.stat_counter import ColumnStatHelper
        if isinstance(column.expr, StarOperator):
            column = Column(Literal(1))
        super(Count, self).__init__(column)
        self.column = column
        self.stat_helper = ColumnStatHelper(column)

    def eval(self, row, schema):
        return self.stat_helper.count

    def __str__(self):
        return "count({0})".format(self.column)


class Max(SimpleStatAggregation):
    def eval(self, row, schema):
        return self.stat_helper.max

    def __str__(self):
        return "max({0})".format(self.column)


class Min(SimpleStatAggregation):
    def eval(self, row, schema):
        return self.stat_helper.min

    def __str__(self):
        return "min({0})".format(self.column)


class Sum(SimpleStatAggregation):
    def eval(self, row, schema):
        return self.stat_helper.sum

    def __str__(self):
        return "sum({0})".format(self.column)


class Avg(SimpleStatAggregation):
    def eval(self, row, schema):
        return self.stat_helper.mean

    def __str__(self):
        return "avg({0})".format(self.column)
