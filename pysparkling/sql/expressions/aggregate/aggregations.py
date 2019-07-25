from pysparkling.sql.expressions.expressions import Expression


class Aggregation(Expression):
    def merge(self, row, schema):
        raise NotImplementedError

    def mergeStats(self, other):
        raise NotImplementedError

    def eval(self, row, schema):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError

