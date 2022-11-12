import collections
import csv
import json
import os
import shutil

from ...utils import get_json_encoder, portable_hash
from ..casts import cast_to_string
from ..expressions.aggregate.aggregations import Aggregation
from ..expressions.mappers import StarOperator
from ..functions import col
from ..internal_utils.options import Options
from ..internal_utils.readwrite import to_option_stored_value
from ..types import Row
from ..utils import AnalysisException


class InternalWriter:
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
        super().__init__()
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
        return f"write_in_folder({self.column})"

    def args(self):
        return (self.column,)


class DataWriter:
    default_options = dict(
        dateFormat="yyyy-MM-dd",
        timestampFormat="yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    )

    def __init__(self, df, mode, options, partitioning_col_names, num_buckets,
                 bucket_col_names, sort_col_names):
        """

        :param df: pysparkling.sql.DataFrame
        :param mode: str
        :param options: Dict[str, Optional[str]]
        :param partitioning_col_names: Optional[List[str]]
        :param num_buckets: Optional[int]
        :param bucket_col_names: Optional[List[str]]
        :param sort_col_names: Optional[List[str]]
        """
        self.mode = mode
        self.options = Options(self.default_options, options)
        self.partitioning_col_names = partitioning_col_names if partitioning_col_names else []
        self.num_buckets = num_buckets
        self.bucket_col_names = bucket_col_names if partitioning_col_names else []
        self.sort_col_names = sort_col_names if partitioning_col_names else []
        if self.partitioning_col_names:
            self.apply_on_aggregated_data = df.groupBy(*self.partitioning_col_names).agg
        else:
            self.apply_on_aggregated_data = df.select

    @property
    def path(self):
        return self.options["path"].rstrip("/")

    @property
    def compression(self):
        return None

    @property
    def encoding(self):
        return None

    def save(self):
        output_path = self.path
        mode = self.mode
        if os.path.exists(output_path):
            if mode == "ignore":
                return
            if mode in ("error", "errorifexists"):
                raise AnalysisException(f"path {output_path} already exists.;")
            if mode == "overwrite":
                shutil.rmtree(output_path)
                os.makedirs(output_path)
        else:
            os.makedirs(output_path)

        self.apply_on_aggregated_data(col(WriteInFolder(writer=self))).collect()

        success_path = os.path.join(output_path, "_SUCCESS")

        with open(success_path, "w", encoding="utf8"):
            pass

    def preformat(self, row, schema):
        raise NotImplementedError

    def write(self, items, ref_value, schema):
        """
        Write a list of rows (items) which have a given schema

        Returns the number of rows written
        """
        raise NotImplementedError


class CSVWriter(DataWriter):
    def check_options(self):
        unsupported_options = {
            "compression",
            "encoding",
            "chartoescapequoteescaping",
            "escape",
            "escapequotes"
        }
        options_requested_but_not_supported = set(self.options) & unsupported_options
        if options_requested_but_not_supported:
            raise NotImplementedError(
                "Pysparkling does not support yet the following options:"
                f" {options_requested_but_not_supported}"
            )

    def preformat_cell(self, value, field):
        if value is None:
            value = self.nullValue
        else:
            value = cast_to_string(
                value,
                from_type=field.dataType,
                options=self.options
            )
        if self.ignoreLeadingWhiteSpace:
            value = value.rstrip()
        if self.ignoreTrailingWhiteSpace:
            value = value.lstrip()
        if value == "":
            return self.emptyValue
        return value

    def preformat(self, row, schema):
        return tuple(
            self.preformat_cell(value, field)
            for value, field in zip(row, schema.fields)
        )

    @property
    def sep(self):
        return self.options.get("sep", ",")

    @property
    def quote(self):
        quote = self.options.get("quote", '"')
        return "\u0000" if quote == "" else quote

    @property
    def escape(self):
        return self.options.get("escape", "\\")

    @property
    def header(self):
        return self.options.get("header", "false") != "false"

    @property
    def nullValue(self):
        return self.options.get("nullvalue", "")

    @property
    def escapeQuotes(self):
        return self.options.get("escapequotes", "true") != "false"

    @property
    def quoteAll(self):
        return self.options.get("quoteall", "false") != "false"

    @property
    def ignoreLeadingWhiteSpace(self):
        return self.options.get("ignoreleadingwhiteSpace", 'false') != "false"

    @property
    def ignoreTrailingWhiteSpace(self):
        return self.options.get("ignoretrailingwhiteSpace", 'false') != "false"

    @property
    def charToEscapeQuoteEscaping(self):
        return None

    @property
    def emptyValue(self):
        return self.options.get("emptyvalue", '""')

    @property
    def lineSep(self):
        return self.options.get("linesep", "\n")

    def write(self, items, ref_value, schema):
        self.check_options()
        output_path = self.path

        if not items:
            return 0

        partition_parts = [
            f"{col_name}={ref_value[col_name]}"
            for col_name in self.partitioning_col_names
        ]
        file_path = "/".join(
            [output_path]
            + partition_parts
            + [f"part-00000-{portable_hash(ref_value)}.csv"]
        )

        # pylint: disable=W0511
        # todo: Add support of:
        #  - all files systems (not only local)
        #  - compression
        #  - encoding
        #  - charToEscapeQuoteEscaping
        #  - escape
        #  - escapeQuotes

        with open(file_path, "w", encoding="utf8") as f:
            writer = csv.writer(
                f,
                delimiter=self.sep,
                quotechar=self.quote,
                quoting=csv.QUOTE_ALL if self.quoteAll else csv.QUOTE_MINIMAL,
                lineterminator=self.lineSep
            )
            if self.header:
                writer.writerow(schema.names)
            writer.writerows(items)
        return len(items)


class JSONWriter(DataWriter):
    def __init__(self, df, mode, options, partitioning_col_names, num_buckets,
                 bucket_col_names, sort_col_names):
        super().__init__(df, mode, options, partitioning_col_names, num_buckets,
                         bucket_col_names, sort_col_names)

        self.encoder = get_json_encoder(self.options)

    def check_options(self):
        unsupported_options = {
            "compression",
            "encoding",
            "chartoescapequoteescaping",
            "escape",
            "escapequotes"
        }
        options_requested_but_not_supported = set(self.options) & unsupported_options
        if options_requested_but_not_supported:
            raise NotImplementedError(
                "Pysparkling does not support yet the following options:"
                f" {options_requested_but_not_supported}"
            )

    @property
    def lineSep(self):
        return self.options.get("linesep", "\n")

    def preformat(self, row, schema):
        return json.dumps(
            collections.OrderedDict(zip(schema.names, row)),
            cls=self.encoder,
            separators=(',', ':')
        ) + self.lineSep

    def write(self, items, ref_value, schema):
        self.check_options()
        output_path = self.path

        if not items:
            return 0

        partition_parts = [
            f"{col_name}={ref_value[col_name]}"
            for col_name in self.partitioning_col_names
        ]
        partition_folder: str = "/".join([output_path] + partition_parts)
        file_path = f"{partition_folder}/part-00000-{portable_hash(ref_value)}.json"

        if not os.path.exists(partition_folder):
            os.makedirs(partition_folder)

        with open(file_path, "a", encoding="utf8") as f:
            f.writelines(items)
        return len(items)
