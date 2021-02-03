from ....stat_counter import ColumnStatHelper
from ...column import Column
from ..literals import Literal
from ..mappers import StarOperator
from .aggregations import Aggregation


class SimpleStatAggregation(Aggregation):
    def __init__(self, column):
        super().__init__(column)
        self.column = column
        self.stat_helper = ColumnStatHelper(column)

    def merge(self, row, schema):
        self.stat_helper.merge(row, schema)

    def mergeStats(self, other, schema):
        self.stat_helper.mergeStats(other.stat_helper)

    def eval(self, row, schema):
        raise NotImplementedError

    def args(self):
        return (self.column,)


class Count(SimpleStatAggregation):
    pretty_name = "count"

    def __init__(self, column):
        if isinstance(column.expr, StarOperator):
            column = Column(Literal(1))
        super().__init__(column)
        self.column = column
        self.stat_helper = ColumnStatHelper(column)

    def eval(self, row, schema):
        return self.stat_helper.count


class Max(SimpleStatAggregation):
    pretty_name = "max"

    def eval(self, row, schema):
        return self.stat_helper.max


class Min(SimpleStatAggregation):
    pretty_name = "min"

    def eval(self, row, schema):
        return self.stat_helper.min


class Sum(SimpleStatAggregation):
    pretty_name = "sum"

    def eval(self, row, schema):
        return self.stat_helper.sum


class Avg(SimpleStatAggregation):
    pretty_name = "avg"

    def eval(self, row, schema):
        return self.stat_helper.mean


class VarSamp(SimpleStatAggregation):
    pretty_name = "var_samp"

    def eval(self, row, schema):
        return self.stat_helper.variance_samp


class VarPop(SimpleStatAggregation):
    pretty_name = "var_pop"

    def eval(self, row, schema):
        return self.stat_helper.variance_pop


class StddevSamp(SimpleStatAggregation):
    pretty_name = "stddev_samp"

    def eval(self, row, schema):
        return self.stat_helper.stddev_samp


class StddevPop(SimpleStatAggregation):
    pretty_name = "stddev_pop"

    def eval(self, row, schema):
        return self.stat_helper.stddev_pop


class Skewness(SimpleStatAggregation):
    pretty_name = "skewness"

    def eval(self, row, schema):
        return self.stat_helper.skewness


class Kurtosis(SimpleStatAggregation):
    pretty_name = "kurtosis"

    def eval(self, row, schema):
        return self.stat_helper.kurtosis


__all__ = [
    "Avg", "VarPop", "VarSamp", "Sum", "StddevPop", "StddevSamp",
    "Skewness", "Min", "Max", "Kurtosis", "Count"
]
