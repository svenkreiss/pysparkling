from pysparkling.sql.expressions.aggregate.aggregations import Aggregation


class SimpleStatAggregation(Aggregation):
    def __init__(self, column):
        from pysparkling.stat_counter import ColumnStatHelper
        super().__init__(column)
        self.column = column
        self.stat_helper = ColumnStatHelper(column)

    def merge(self, row, schema):
        self.stat_helper.merge(row, schema)

    def mergeStats(self, other, schema):
        self.stat_helper.mergeStats(other.expr.stat_helper)

    def eval(self, row, schema):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError


class Count(SimpleStatAggregation):
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


class VarSamp(SimpleStatAggregation):
    def eval(self, row, schema):
        return self.stat_helper.variance_samp

    def __str__(self):
        return "var_samp({0})".format(self.column)


class VarPop(SimpleStatAggregation):
    def eval(self, row, schema):
        return self.stat_helper.variance_pop

    def __str__(self):
        return "var_pop({0})".format(self.column)


class StddevSamp(SimpleStatAggregation):
    def eval(self, row, schema):
        return self.stat_helper.stddev_samp

    def __str__(self):
        return "stddev_samp({0})".format(self.column)


class StddevPop(SimpleStatAggregation):
    def eval(self, row, schema):
        return self.stat_helper.stddev_pop

    def __str__(self):
        return "stddev_pop({0})".format(self.column)


class Skewness(SimpleStatAggregation):
    def eval(self, row, schema):
        return self.stat_helper.skewness

    def __str__(self):
        return "skewness({0})".format(self.column)


class Kurtosis(SimpleStatAggregation):
    def eval(self, row, schema):
        return self.stat_helper.kurtosis

    def __str__(self):
        return "kurtosis({0})".format(self.column)
