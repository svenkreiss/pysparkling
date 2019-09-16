import itertools
import json

from pysparkling.fileio import TextFile, File
from pysparkling.sql.casts import *
from pysparkling.sql.internal_utils.readwrite import OptionUtils, to_option_stored_value
from pysparkling.sql.internals import DataFrameInternal
from pysparkling.sql.schema_utils import infer_schema_from_rdd
from pysparkling.sql.utils import AnalysisException
from pysparkling.utils import row_from_keyed_values


class InternalReader(OptionUtils):
    def schema(self, schema):
        self._schema = schema

    def option(self, k, v):
        self._options[k.lower()] = to_option_stored_value(v)

    def __init__(self, spark):
        """

        :type spark: pysparkling.sql.session.SparkSession
        """
        self._spark = spark
        self._options = {}
        self._schema = None

    def csv(self, paths):
        return CSVReader(self._spark, paths, self._schema, self._options).read()

    def json(self, paths):
        return JSONReader(self._spark, paths, self._schema, self._options).read()


class CSVReader(object):
    def __init__(self, spark, paths, schema, options):
        self.spark = spark
        self.paths = paths
        self.schema = schema
        self.sep = options.get("sep", ",")
        self.linesep = options.get("linesep", None)
        self.encoding = options.get("encoding", "utf-8")
        self.inferSchema = str(options.get("inferSchema", False)).lower() != "false"

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
            self.sep,
            self.linesep,
            self.encoding
        ))

        partitions_fields = partition_schema.fields if partition_schema is not None else []
        if self.schema is not None:
            schema = self.schema
        elif self.inferSchema:
            schema = guess_schema_from_strings(rdd.take(1)[0].__fields__, rdd.collect())
        else:
            schema = infer_schema_from_rdd(rdd)

        schema_with_string = StructType(fields=[
            StructField(field.name, StringType()) for field in schema.fields
        ])
        full_schema = StructType(schema.fields[:-len(partitions_fields)] + partitions_fields)

        cast_row = get_caster(from_type=schema_with_string, to_type=full_schema)
        casted_rdd = rdd.map(cast_row)
        casted_rdd._name = paths

        return DataFrameInternal(
            sc,
            casted_rdd,
            schema=full_schema
        )


def resolve_partitions(patterns):
    """
    Given a list of patterns, returns all the files matching or in folders matching
    one of them.

    The file are returned in a list of tuple of 2 elements:
    - The first tuple is the file path
    - The second being the partition keys and values if any were encountered else None

    In addition to this list, return, if the data was partitioned, a schema for the
    partition keys, else None

    :type patterns: list of str
    :rtype: Tuple[List[str], List[Optional[Row]], Optional[StructType]]
    """
    file_paths = File.get_content(patterns)
    if not file_paths:
        raise AnalysisException('Path does not exist:'.format(patterns))
    partitions = {}
    for file_path in file_paths:
        if "=" in file_path:
            row = row_from_keyed_values(
                folder.split("=")
                for folder in file_path.split("/")[:-1]
                if folder.count("=") == 1
            )
            partitions[file_path] = row
        else:
            partitions[file_path] = None

    if any(value is None for value in partitions.values()):
        raise AnalysisException(
            "Unable to parse those malformed folders: {1}".format(
                file_paths,
                [path for path, value in partitions.items() if value is None]
            )
        )

    partitioning_field_sets = set(p.__fields__ for p in partitions.values())
    if len(partitioning_field_sets) > 1:
        raise Exception(
            "Conflicting directory structures detected while reading {0}. "
            "All partitions must have the same partitioning fields, found fields {1}".format(
                ",".join(patterns),
                " and also ".join(
                    str(fields) for fields in partitioning_field_sets
                )
            )
        )

    if partitioning_field_sets:
        partitioning_fields = partitioning_field_sets.pop()
        partition_schema = guess_schema_from_strings(partitioning_fields, partitions.values())
    else:
        partition_schema = None

    return partitions, partition_schema


def guess_schema_from_strings(schema_fields, data):
    field_values = {
        field: [row[field] for row in data]
        for field in schema_fields
    }

    field_types_and_values = {
        field: guess_type_from_values_as_string(values)
        for field, values in field_values.items()
    }

    schema = StructType(fields=[
        StructField(field, field_type)
        for field, field_type in field_types_and_values.items()
    ])

    return schema


def guess_type_from_values_as_string(values):
    # Reproduces inferences available in Spark
    # PartitioningUtils.inferPartitionColumnValue()
    # located in org.apache.spark.sql.execution.datasources
    tested_types = (
        IntegerType(),
        LongType(),
        DecimalType(),
        DoubleType(),
        TimestampType(),
        DateType(),
        StringType()
    )
    for tested_type in tested_types:
        string_type = StringType()
        type_caster = get_caster(from_type=string_type, to_type=tested_type)
        try:
            for value in values:
                casted_value = type_caster(value)
                if casted_value is None and value != "null":
                    raise ValueError
            return tested_type
        except ValueError:
            pass
    # Should never happen
    raise AnalysisException("Unable to find a matching type for some partition, even StringType did not work")


def parse_csv_file(partitions, partition_schema, schema, sep, linesep, encoding, f_name):
    f_content = TextFile(f_name).load(encoding=encoding).read()
    records = f_content.split(linesep) if linesep is not None else f_content.splitlines()
    rows = []
    for record in records:
        record_values = record.split(sep)
        if schema is not None:
            field_names = [f.name for f in schema.fields]
        else:
            field_names = ["_c{0}".format(i) for i, field in enumerate(record_values)]
        partition_field_names = [f.name for f in partition_schema.fields]
        row = row_from_keyed_values(zip(
            itertools.chain(field_names, partition_field_names),
            itertools.chain(record_values, partitions[f_name])
        ))
        rows.append(row)
    return rows


class JSONReader(object):
    def __init__(self, spark, paths, schema, options):
        self.spark = spark
        self.paths = paths
        self.schema = schema

    def read(self):
        sc = self.spark._sc
        paths = self.paths
        if self.schema is None:
            raise NotImplementedError
        cols = _parse_schema(self.schema)
        return DataFrameInternal(
            sc,
            sc.textFile(",".join(paths)).map(lambda line: json.loads(line).values()),
            convert_to_row=True,
            cols=cols
        )


def _parse_schema(schema):
    """ Temporary (ugly) simple function """
    if schema is None:
        return None
    return (schema.replace("\n", "")
            .replace(r" ", "")
            .replace("STRING", "")
            .replace("string", "")
            .split(",")
            )
