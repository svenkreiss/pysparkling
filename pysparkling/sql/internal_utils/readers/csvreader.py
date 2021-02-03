from functools import partial
import itertools

from pysparkling.fileio import TextFile
from pysparkling.sql.casts import get_caster
from pysparkling.sql.internal_utils.options import Options
from pysparkling.sql.internal_utils.readers.utils import guess_schema_from_strings, resolve_partitions
from pysparkling.sql.internals import DataFrameInternal
from pysparkling.sql.schema_utils import infer_schema_from_rdd
from pysparkling.sql.types import create_row, StringType, StructField, StructType


class CSVReader:
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
            fields = rdd.take(1)[0].__fields__
            schema = guess_schema_from_strings(fields, rdd.collect(), options=self.options)
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

        cast_row = get_caster(
            from_type=schema_with_string, to_type=full_schema, options=self.options
        )
        casted_rdd = rdd.map(cast_row)
        casted_rdd._name = paths

        return DataFrameInternal(
            sc,
            casted_rdd,
            schema=full_schema
        )


def parse_csv_file(partitions, partition_schema, schema, options, file_name):
    f_content = TextFile(file_name).load(encoding=options.encoding).read()
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
        row = csv_record_to_row(
            record, options, schema, header, null_value, partition_schema, partitions[file_name]
        )
        row.set_input_file_name(file_name)
        rows.append(row)

    return rows


def csv_record_to_row(record, options, schema=None, header=None,
                      null_value=None, partition_schema=None, partition=None):
    record_values = [val if val != null_value else None for val in record.split(options.sep)]
    if schema is not None:
        field_names = [f.name for f in schema.fields]
    elif header is not None:
        field_names = header
    else:
        field_names = ["_c{0}".format(i) for i, field in enumerate(record_values)]
    partition_field_names = [
        f.name for f in partition_schema.fields
    ] if partition_schema else []
    row = create_row(
        itertools.chain(field_names, partition_field_names),
        itertools.chain(record_values, partition or [])
    )
    return row
