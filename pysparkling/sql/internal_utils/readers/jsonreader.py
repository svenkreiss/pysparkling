import itertools
import json
from functools import partial

from pysparkling.sql.casts import get_struct_caster
from pysparkling.sql.internal_utils.options import Options
from pysparkling.sql.internal_utils.readers.utils import get_records, resolve_partitions
from pysparkling.sql.internals import DataFrameInternal
from pysparkling.sql.schema_utils import infer_schema_from_rdd
from pysparkling.sql.types import create_row, row_from_keyed_values, StructType


class JSONReader(object):
    default_options = dict(
        primitivesAsString=False,
        prefersDecimal=False,
        allowComments=False,
        allowUnquotedFieldNames=False,
        allowSingleQuotes=True,
        allowNumericLeadingZero=False,
        allowBackslashEscapingAnyCharacter=False,
        mode="PERMISSIVE",
        columnNameOfCorruptRecord="",
        dateFormat="yyyy-MM-dd",
        timestampFormat="yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        multiLine=False,
        allowUnquotedControlChars=False,
        encoding=None,
        lineSep=None,
        samplingRatio=1.0,
        dropFieldIfAllNull=False,
        locale="en-US",
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
            parse_json_file,
            partitions,
            partition_schema,
            self.schema,
            self.options
        ))

        inferred_schema = infer_schema_from_rdd(rdd)

        schema = self.schema if self.schema is not None else inferred_schema
        schema_fields = {
            field.name: field
            for field in schema.fields
        }

        # Field order is defined by fields in the record, not by the given schema
        # Field type is defined by the given schema or inferred
        full_schema = StructType(
            fields=[
                schema_fields.get(field.name, field)
                for field in inferred_schema.fields
            ]
        )

        cast_row = get_struct_caster(inferred_schema, full_schema, options=self.options)
        casted_rdd = rdd.map(cast_row)
        casted_rdd._name = paths

        return DataFrameInternal(
            sc,
            casted_rdd,
            schema=full_schema
        )


def parse_json_file(partitions, partition_schema, schema, options, file_name):
    records = get_records(file_name, options.linesep, options.encoding)
    rows = []
    for record in records:
        partition = partitions[file_name]
        row = parse_record(record, schema, partition, partition_schema, options)
        row.set_input_file_name(file_name)
        rows.append(row)
    return rows


def parse_record(record, schema, partition, partition_schema, options):
    raw_record_value = json.loads(record, encoding=options.encoding)
    if not isinstance(raw_record_value, dict):
        raise NotImplementedError(
            "Top level items should be JSON objects (dicts), got {0} with {1}".format(
                type(raw_record_value),
                raw_record_value
            )
        )
    record_value = decode_record(raw_record_value)
    if schema is not None:
        record_fields = record_value.__fields__
        available_names = tuple(partition_schema.names) + record_fields
        field_names = [name for name in record_fields if name in schema.names] + [
            f.name for f in schema.fields if f.name not in available_names
        ]
    else:
        field_names = list(record_value.__fields__)
    record_values = [
        record_value[field_name] if field_name in record_value.__fields__ else None
        for field_name in field_names
    ]
    partition_field_names = [f.name for f in partition_schema.fields] if partition_schema else []
    # pylint: disable=W0511
    # todo: handle nested rows
    row = create_row(
        itertools.chain(field_names, partition_field_names),
        itertools.chain(record_values, partition)
    )
    return row


def decode_record(item):
    if isinstance(item, list):
        return [decode_record(e) for e in item]
    if isinstance(item, dict):
        return row_from_keyed_values(
            (key, decode_record(value))
            for key, value in item.items()
        )
    return item
