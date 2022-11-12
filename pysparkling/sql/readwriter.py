from .dataframe import DataFrame
from .internal_utils.readers import InternalReader
from .internal_utils.readwrite import OptionUtils
from .internal_utils.writers import CSVWriter, InternalWriter, JSONWriter
from .utils import IllegalArgumentException

WRITE_MODES = ("overwrite", "append", "ignore", "error", "errorifexists")
DATA_WRITERS = dict(
    csv=CSVWriter,
    json=JSONWriter,
)


class DataFrameReader(OptionUtils):
    def option(self, key, value):
        self._jreader.option(key, value)
        return self

    def schema(self, schema):
        self._jreader.schema(schema)
        return self

    def __init__(self, spark):
        """

        :type spark: pysparkling.sql.session.SparkSession
        """
        self._jreader = InternalReader(spark)
        self._spark = spark

    def _df(self, jdf):
        return DataFrame(jdf, self._spark)

    def _generic_read(self, path, method_to_read_if_rdd):
        if isinstance(path, str):
            path = [path]

        if isinstance(path, list):
            return self._df(method_to_read_if_rdd(path))

        # pylint: disable=import-outside-toplevel, cyclic-import
        from ..rdd import RDD

        if isinstance(path, RDD):
            return self._df(method_to_read_if_rdd(path.collect()))

        raise TypeError("path can be only string, list or RDD")

    # pylint: disable=R0914
    def csv(self, path, schema=None, sep=None, encoding=None, quote=None, escape=None,
            comment=None, header=None, inferSchema=None, ignoreLeadingWhiteSpace=None,
            ignoreTrailingWhiteSpace=None, nullValue=None, nanValue=None, positiveInf=None,
            negativeInf=None, dateFormat=None, timestampFormat=None, maxColumns=None,
            maxCharsPerColumn=None, maxMalformedLogPerPartition=None, mode=None,
            columnNameOfCorruptRecord=None, multiLine=None, charToEscapeQuoteEscaping=None,
            samplingRatio=None, enforceSchema=None, emptyValue=None, locale=None, lineSep=None):
        self._set_opts(
            schema=schema, sep=sep, encoding=encoding, quote=quote, escape=escape, comment=comment,
            header=header, inferSchema=inferSchema, ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace, nullValue=nullValue,
            nanValue=nanValue, positiveInf=positiveInf, negativeInf=negativeInf,
            dateFormat=dateFormat, timestampFormat=timestampFormat, maxColumns=maxColumns,
            maxCharsPerColumn=maxCharsPerColumn,
            maxMalformedLogPerPartition=maxMalformedLogPerPartition, mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord, multiLine=multiLine,
            charToEscapeQuoteEscaping=charToEscapeQuoteEscaping, samplingRatio=samplingRatio,
            enforceSchema=enforceSchema, emptyValue=emptyValue, locale=locale, lineSep=lineSep
        )

        return self._generic_read(path, self._jreader.csv)

    # pylint: disable=R0914
    def json(self, path, schema=None, primitivesAsString=None, prefersDecimal=None,
             allowComments=None, allowUnquotedFieldNames=None, allowSingleQuotes=None,
             allowNumericLeadingZero=None, allowBackslashEscapingAnyCharacter=None,
             mode=None, columnNameOfCorruptRecord=None, dateFormat=None, timestampFormat=None,
             multiLine=None, allowUnquotedControlChars=None, lineSep=None, samplingRatio=None,
             dropFieldIfAllNull=None, encoding=None, locale=None):
        self._set_opts(
            schema=schema, primitivesAsString=primitivesAsString, prefersDecimal=prefersDecimal,
            allowComments=allowComments, allowUnquotedFieldNames=allowUnquotedFieldNames,
            allowSingleQuotes=allowSingleQuotes, allowNumericLeadingZero=allowNumericLeadingZero,
            allowBackslashEscapingAnyCharacter=allowBackslashEscapingAnyCharacter,
            mode=mode, columnNameOfCorruptRecord=columnNameOfCorruptRecord, dateFormat=dateFormat,
            timestampFormat=timestampFormat, multiLine=multiLine,
            allowUnquotedControlChars=allowUnquotedControlChars, lineSep=lineSep,
            samplingRatio=samplingRatio, dropFieldIfAllNull=dropFieldIfAllNull, encoding=encoding,
            locale=locale)

        return self._generic_read(path, self._jreader.json)

    def text(self, path, wholetext=False, lineSep=None,
             pathGlobFilter=None, recursiveFileLookup=None):
        self._set_opts(wholetext=wholetext,
                       lineSep=lineSep,
                       pathGlobFilter=pathGlobFilter,
                       recursiveFileLookup=recursiveFileLookup)

        return self._generic_read(path, self._jreader.text)


class DataFrameWriter(OptionUtils):
    def __init__(self, df):
        self._df = df
        self._spark = df.sql_ctx
        self._jwrite = InternalWriter(self._df)

    def mode(self, saveMode):
        if saveMode is None:
            return self
        if saveMode not in WRITE_MODES:
            accepted_modes = "', '".join(WRITE_MODES)
            raise IllegalArgumentException(
                f"Unknown save mode: {saveMode}. Accepted save modes are {accepted_modes}."
            )
        self._jwrite = self._jwrite.mode(saveMode)
        return self

    def format(self, source):
        self._jwrite = self._jwrite.format(source)
        return self

    def option(self, key, value):
        self._jwrite.option(key, value)
        return self

    def options(self, **options):
        for k, v in options.items():
            self._jwrite.option(k, v)
        return self

    def partitionBy(self, *cols):
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]
        self._jwrite = self._jwrite.partitionBy(cols)
        return self

    def bucketBy(self, numBuckets, col, *cols):
        if not isinstance(numBuckets, int):
            raise TypeError(f"numBuckets should be an int, got {type(numBuckets)}.")

        if isinstance(col, (list, tuple)):
            if cols:
                raise ValueError(f"col is a {type(col)} but cols are not empty")

            cols = col
        else:
            cols = [col] + list(cols)

        if not all(isinstance(c, str) for c in cols):
            raise TypeError("all names should be `str`")

        self._jwrite = self._jwrite.bucketBy(numBuckets, *cols)
        return self

    def sortBy(self, col, *cols):
        if isinstance(col, (list, tuple)):
            if cols:
                raise ValueError(f"col is a {type(col)} but cols are not empty")

            col, cols = col[0], col[1:]

        if not all(isinstance(c, str) for c in cols) or not isinstance(col, str):
            raise TypeError("all names should be `str`")

        self._jwrite = self._jwrite.sortBy(col, *cols)
        return self

    # noinspection PyShadowingBuiltins
    # pylint: disable=W0622
    def save(self, path=None, format=None, mode=None, partitionBy=None, **options):
        self.mode(mode).options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        writer_class = DATA_WRITERS[self._jwrite._source]
        if path is None:
            self._jwrite.save(writer_class)
        else:
            self._jwrite.save(writer_class, path)

    def json(self, path, mode=None, compression=None, dateFormat=None, timestampFormat=None,
             lineSep=None, encoding=None):
        self.mode(mode)
        self._set_opts(
            compression=compression, dateFormat=dateFormat, timestampFormat=timestampFormat,
            lineSep=lineSep, encoding=encoding)
        self.format("json").save(path)

    # pylint: disable=R0914
    def csv(self, path, mode=None, compression=None, sep=None, quote=None, escape=None,
            header=None, nullValue=None, escapeQuotes=None, quoteAll=None, dateFormat=None,
            timestampFormat=None, ignoreLeadingWhiteSpace=None, ignoreTrailingWhiteSpace=None,
            charToEscapeQuoteEscaping=None, encoding=None, emptyValue=None, lineSep=None):
        self.mode(mode)
        self._set_opts(compression=compression, sep=sep, quote=quote, escape=escape, header=header,
                       nullValue=nullValue, escapeQuotes=escapeQuotes, quoteAll=quoteAll,
                       dateFormat=dateFormat, timestampFormat=timestampFormat,
                       ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
                       ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace,
                       charToEscapeQuoteEscaping=charToEscapeQuoteEscaping,
                       encoding=encoding, emptyValue=emptyValue, lineSep=lineSep)
        self.format("csv").save(path)

    def insertInto(self, tableName=None):
        raise NotImplementedError("Pysparkling does not implement write to table yet")

    def saveAsTable(self, name):
        raise NotImplementedError("Pysparkling does not implement write to table yet")

    def parquet(self, path):
        raise NotImplementedError("Pysparkling does not implement write to parquet yet")

    def text(self, path):
        raise NotImplementedError("Pysparkling does not implement write to text yet")

    def orc(self, path):
        raise NotImplementedError("Pysparkling does not implement write to ORC")

    def jdbc(self, path):
        raise NotImplementedError("Pysparkling does not implement write to JDBC")
