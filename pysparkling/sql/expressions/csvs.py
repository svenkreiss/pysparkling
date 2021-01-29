from pysparkling.sql.casts import NO_TIMESTAMP_CONVERSION
from pysparkling.sql.expressions.expressions import Expression
from pysparkling.sql.internal_utils.options import Options
from pysparkling.sql.utils import AnalysisException

sql_csv_function_options = dict(
    dateFormat=NO_TIMESTAMP_CONVERSION,
    timestampFormat=NO_TIMESTAMP_CONVERSION,
)


class SchemaOfCsv(Expression):
    pretty_name = "schema_of_csv"

    def __init__(self, column, options):
        super().__init__(column)
        self.column = column
        self.input_options = options
        # pylint: disable=import-outside-toplevel; circular import
        from pysparkling.sql.internal_utils.readers.csvreader import CSVReader
        self.options = Options(CSVReader.default_options, sql_csv_function_options, options)

    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        if not isinstance(value, str) or value == "":
            raise AnalysisException(
                "type mismatch: The input csv should be a string literal and not null; "
                "however, got {0}.".format(value)
            )
        # pylint: disable=import-outside-toplevel; circular import
        from pysparkling.sql.internal_utils.readers.csvreader import csv_record_to_row
        from pysparkling.sql.internal_utils.readers.utils import guess_schema_from_strings

        record_as_row = csv_record_to_row(value, self.options)
        schema = guess_schema_from_strings(record_as_row.__fields__, [record_as_row], self.options)
        return schema.simpleString()

    def args(self):
        return (self.column,)
