import itertools
from functools import partial

from pysparkling.fileio import TextFile
from pysparkling.sql.internal_utils.options import Options
from pysparkling.sql.internal_utils.readers.utils import resolve_partitions
from pysparkling.sql.internals import DataFrameInternal
from pysparkling.sql.types import StringType, StructField, StructType, create_row


class TextReader(object):
    default_options = dict(
        lineSep=None,
        encoding="utf-8",
        sep=",",
        inferSchema=False,
        header=False
    )

    def __init__(self, spark, paths, schema, options):
        self.spark = spark
        self.paths = paths
        self.schema = schema or StructType([StructField("value", StringType())])
        self.options = Options(self.default_options, options)

    def read(self):
        sc = self.spark._sc
        paths = self.paths

        partitions, partition_schema = resolve_partitions(paths)

        rdd_filenames = sc.parallelize(sorted(partitions.keys()), len(partitions))
        rdd = rdd_filenames.flatMap(partial(
            parse_text_file,
            partitions,
            partition_schema,
            self.schema,
            self.options
        ))

        if partition_schema:
            partitions_fields = partition_schema.fields
            full_schema = StructType(self.schema.fields + partitions_fields)
        else:
            full_schema = self.schema

        rdd._name = paths

        return DataFrameInternal(
            sc,
            rdd,
            schema=full_schema
        )


def parse_text_file(partitions, partition_schema, schema, options, file_name):
    f_content = TextFile(file_name).load(encoding=options.encoding).read()
    records = (f_content.split(options.lineSep)
               if options.lineSep is not None
               else f_content.splitlines())

    rows = []
    for record in records:
        row = text_record_to_row(record, options, schema, partition_schema, partitions[file_name])
        row.set_input_file_name(file_name)
        rows.append(row)

    return rows


def text_record_to_row(record, options, schema, partition_schema, partition):
    partition_field_names = [
        f.name for f in partition_schema.fields
    ] if partition_schema else []
    row = create_row(
        itertools.chain([schema.fields[0].name], partition_field_names),
        itertools.chain([record], partition or [])
    )
    return row
