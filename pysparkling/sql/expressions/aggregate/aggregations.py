from pysparkling.sql.expressions.expressions import Expression


class Aggregation(Expression):
    def merge(self, row):
        raise NotImplementedError

    def mergeStats(self, other):
        raise NotImplementedError

    def eval(self, row):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError

