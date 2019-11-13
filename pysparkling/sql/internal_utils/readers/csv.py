import itertools
from functools import partial

from pysparkling.fileio import TextFile
from pysparkling.sql.casts import get_caster
from pysparkling.sql.internal_utils.options import Options
from pysparkling.sql.internal_utils.readers.utils import resolve_partitions, \
    guess_schema_from_strings
from pysparkling.sql.internals import DataFrameInternal
from pysparkling.sql.schema_utils import infer_schema_from_rdd
from pysparkling.sql.types import StructType, StringType, StructField
from pysparkling.utils import row_from_keyed_values


class CSVReader(object):
    default_options = dict(
        lineSep=None,
        encoding="utf-8",
        sep=",",
        inferSchema=False
    )

    def __init__(self, spark, paths, schema, options):
        self.spark = spark
        self.paths = paths
        self.schema = schema
        self.options = Options(self.default_options, options)

    def read(self):
        sc = self.spark._sc
        paths = self.paths

        partitions, partition_schema = resolve_partitions(paths)

        rdd_filenames = sc.parallelize(sorted(partitions.keys()), len(partitions))
        rdd = rdd_filenames.flatMap(partial(
            parse_csv_file,
            partitions,
            partition_schema,
            self.schema,
            self.options
        ))

        if self.schema is not None:
            schema = self.schema
        elif self.options.inferSchema:
            schema = guess_schema_from_strings(rdd.take(1)[0].__fields__, rdd.collect())
        else:
            schema = infer_schema_from_rdd(rdd)

        schema_with_string = StructType(fields=[
            StructField(field.name, StringType()) for field in schema.fields
        ])

        if partition_schema:
            partitions_fields = partition_schema.fields
            full_schema = StructType(schema.fields[:-len(partitions_fields)] + partitions_fields)
        else:
            full_schema = schema

        cast_row = get_caster(from_type=schema_with_string, to_type=full_schema)
        casted_rdd = rdd.map(cast_row)
        casted_rdd._name = paths

        return DataFrameInternal(
            sc,
            casted_rdd,
            schema=full_schema
        )


def parse_csv_file(partitions, partition_schema, schema, options, f_name):
    f_content = TextFile(f_name).load(encoding=options.encoding).read()
    records = (f_content.split(options.lineSep)
               if options.lineSep is not None
               else f_content.splitlines())
    if options.header == "true":
        header = records[0].split(options.sep)
        records = records[1:]
    else:
        header = None

    null_value = ""
    rows = []
    for record in records:
        record_values = [val if val != null_value else None for val in record.split(options.sep)]
        if schema is not None:
            field_names = [f.name for f in schema.fields]
        elif header is not None:
            field_names = [f for f in header]
        else:
            field_names = ["_c{0}".format(i) for i, field in enumerate(record_values)]
        partition_field_names = [
            f.name for f in partition_schema.fields
        ] if partition_schema else []
        row = row_from_keyed_values(zip(
            itertools.chain(field_names, partition_field_names),
            itertools.chain(record_values, partitions[f_name])
        ))
        rows.append(row)
    return rows
