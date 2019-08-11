from pysparkling.sql.expressions.aggregate.aggregations import Aggregation


class CovarianceStatAggregation(Aggregation):
    def __init__(self, column1, column2):
        from pysparkling.stat_counter import CovarianceCounter
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

    def __str__(self):
        raise NotImplementedError


class Corr(CovarianceStatAggregation):
    def eval(self, row, schema):
        return self.stat_helper.pearson_correlation

    def __str__(self):
        return "corr({0}, {1})".format(self.column1, self.column2)


class CovarSamp(CovarianceStatAggregation):
    def eval(self, row, schema):
        return self.stat_helper.covar_samp

    def __str__(self):
        return "covar_samp({0}, {1})".format(self.column1, self.column2)


class CovarPop(CovarianceStatAggregation):
    def eval(self, row, schema):
        return self.stat_helper.covar_pop

    def __str__(self):
        return "covar_pop({0}, {1})".format(self.column1, self.column2)
