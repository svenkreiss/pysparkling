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

    def args(self):
        return (self.column,)


class Count(SimpleStatAggregation):
    pretty_name = "count"

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
