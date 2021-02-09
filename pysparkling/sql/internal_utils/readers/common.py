from ...internal_utils.readers import csvreader, jsonreader, textreader
from ...internal_utils.readwrite import OptionUtils, to_option_stored_value
from ...types import StructType


class InternalReader(OptionUtils):
    def schema(self, schema):
        if not isinstance(schema, StructType):
            raise NotImplementedError("Pysparkling currently only supports StructType for schemas")
        self._schema = schema

    def option(self, key, value):
        self._options[key.lower()] = to_option_stored_value(value)

    def __init__(self, spark):
        """

        :type spark: pysparkling.sql.session.SparkSession
        """
        self._spark = spark
        self._options = {}
        self._schema = None

    def csv(self, paths):
        return csvreader.CSVReader(self._spark, paths, self._schema, self._options).read()

    def json(self, paths):
        return jsonreader.JSONReader(self._spark, paths, self._schema, self._options).read()

    def text(self, paths):
        return textreader.TextReader(self._spark, paths, self._schema, self._options).read()
