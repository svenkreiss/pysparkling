from pysparkling.sql.internal_utils.readwrite import to_option_stored_value


class InternalWriter(object):
    def __init__(self, df):
        self._df = df
        self._source = "parquet"
        self._mode = "errorifexists"
        self._options = {}
        self._partitioning_col_names = None
        self._num_buckets = None
        self._bucket_col_names = None
        self._sort_col_names = None

    def option(self, k, v):
        self._options[k.lower()] = to_option_stored_value(v)
        return self

    def mode(self, mode):
        self._mode = mode
        return self

    def format(self, source):
        self._source = source
        return self

    def partitionBy(self, partitioning_col_names):
        self._partitioning_col_names = partitioning_col_names
        return self

    def bucketBy(self, num_buckets, *bucket_cols):
        self._num_buckets = num_buckets
        self._bucket_col_names = bucket_cols
        return self

    def sortBy(self, sort_cols):
        self._sort_col_names = sort_cols
        return self

    def save(self, writer_class, path=None):
        self.option("path", path)
        return writer_class(
            self._df,
            self._mode,
            self._options,
            self._partitioning_col_names,
            self._num_buckets,
            self._bucket_col_names,
            self._sort_col_names
        ).save()

