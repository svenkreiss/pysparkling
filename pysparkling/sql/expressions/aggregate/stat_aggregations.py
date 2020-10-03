from pysparkling.sql.expressions.aggregate.aggregations import Aggregation


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
