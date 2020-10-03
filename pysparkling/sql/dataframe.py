_NoValue = object()


class DataFrame(object):
    def __init__(self, jdf, sql_ctx):
        self._jdf = jdf
        self.sql_ctx = sql_ctx

    def collect(self):
        """Returns the number of rows in this :class:`DataFrame`.

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.range(2)
        >>> df.collect()
        [Row(id=0), Row(id=1)]
        """
        return self._jdf.collect()

    @property
    def rdd(self):
        return self._jdf.rdd()

    @property
    def is_cached(self):
        return self._jdf.is_cached()

    def dropna(self, how='any', thresh=None, subset=None):
        if how is not None and how not in ['any', 'all']:
            raise ValueError("how ('" + how + "') should be 'any' or 'all'")

        if subset is None:
            subset = self.columns
        elif isinstance(subset, str):
            subset = [subset]
        elif not isinstance(subset, (list, tuple)):
            raise ValueError("subset should be a list or tuple of column names")

        if thresh is None:
            thresh = len(subset) if how == 'any' else 1

        return DataFrame(self._jdf.dropna(thresh, subset), self.sql_ctx)

