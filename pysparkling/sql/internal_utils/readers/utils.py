from pysparkling.fileio import TextFile
from pysparkling.sql.casts import get_caster
from pysparkling.sql.types import StructType, StructField, IntegerType, LongType, DecimalType, \
    DoubleType, TimestampType, StringType
from pysparkling.sql.utils import AnalysisException


def guess_schema_from_strings(schema_fields, data, options):
    field_values = [
        (field, [row[field] for row in data])
        for field in schema_fields
    ]

    field_types_and_values = [
        (field, guess_type_from_values_as_string(values, options))
        for field, values in field_values
    ]

    schema = StructType(fields=[
        StructField(field, field_type)
        for field, field_type in field_types_and_values
    ])

    return schema


def guess_type_from_values_as_string(values, options):
    # Reproduces inferences available in Spark
    # PartitioningUtils.inferPartitionColumnValue()
    # located in org.apache.spark.sql.execution.datasources
    tested_types = (
        IntegerType(),
        LongType(),
        DecimalType(),
        DoubleType(),
        TimestampType(),
        StringType()
    )
    string_type = StringType()
    for tested_type in tested_types:
        type_caster = get_caster(from_type=string_type, to_type=tested_type, options=options)
        try:
            for value in values:
                casted_value = type_caster(value)
                if casted_value is None and value not in ("null", None):
                    raise ValueError
            return tested_type
        except ValueError:
            pass
    # Should never happen
    raise AnalysisException(
        "Unable to find a matching type for some fields, even StringType did not work"
    )


def get_records(f_name, linesep, encoding):
    f_content = TextFile(f_name).load(encoding=encoding).read()
    records = f_content.split(linesep) if linesep is not None else f_content.splitlines()
    return records
