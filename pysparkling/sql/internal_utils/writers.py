from pysparkling import Row
from pysparkling.sql.expressions.aggregate.aggregations import Aggregation
from pysparkling.sql.expressions.mappers import StarOperator
from pysparkling.sql.functions import col
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


class WriteInFolder(Aggregation):
    """
    This use the computation engine of pysparkling to write the values in a folder.

    It's behaviour is similar to a collect_list except that items are written
    in a folder instead of being returned:

    Its evaluation only return the number of items written while writing them
    using the writer given in the constructor, more specifically its write method.

    Pre-formatting is done as defined by writer.preformat during the merge phase.

    """

    def __init__(self, writer):
        super(WriteInFolder, self).__init__()
        self.column = col(StarOperator())
        self.writer = writer
        self.ref_value = None
        self.items = []

    def merge(self, row, schema):
        row_value = self.column.eval(row, schema)
        if self.ref_value is None:
            ref_value = Row(*row_value)
            ref_value.__fields__ = schema.names
            self.ref_value = ref_value
        self.items.append(
            self.writer.preformat(row_value, schema)
        )

    def mergeStats(self, other, schema):
        self.items += other.items
        if self.ref_value is None:
            self.ref_value = other.ref_value

    def eval(self, row, schema):
        return self.writer.write(
            self.items,
            self.ref_value,
            self.pre_evaluation_schema
        )

    def __str__(self):
        return "write_in_folder({0})".format(self.column)
