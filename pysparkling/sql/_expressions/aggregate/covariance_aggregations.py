from ..._statcounter import CovarianceCounter
from .aggregations import Aggregation


class CovarianceStatAggregation(Aggregation):
    def __init__(self, column1, column2):
        super().__init__(column1, column2)
        self.column1 = column1
        self.column2 = column2
        self.stat_helper = CovarianceCounter(method="pearson")

    def merge(self, row, schema):
        self.stat_helper.add(row.eval(self.column1, schema), row.eval(self.column2, schema))

    def mergeStats(self, other, schema):
        self.stat_helper.merge(other)

    def eval(self, row, schema):
        raise NotImplementedError

    def args(self):
        return (
            self.column1,
            self.column2
        )


class Corr(CovarianceStatAggregation):
    pretty_name = "corr"

    def eval(self, row, schema):
        return self.stat_helper.pearson_correlation


class CovarSamp(CovarianceStatAggregation):
    pretty_name = "covar_samp"

    def eval(self, row, schema):
        return self.stat_helper.covar_samp


class CovarPop(CovarianceStatAggregation):
    pretty_name = "covar_pop"

    def eval(self, row, schema):
        return self.stat_helper.covar_pop


__all__ = ["Corr", "CovarSamp", "CovarPop"]
