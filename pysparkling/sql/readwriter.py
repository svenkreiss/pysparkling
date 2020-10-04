from pysparkling import RDD
from pysparkling.sql.dataframe import DataFrame
from pysparkling.sql.internal_utils.readers import InternalReader
from pysparkling.sql.internal_utils.readwrite import OptionUtils


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
        if isinstance(path, str):
            path = [path]
        if isinstance(path, list):
            return self._df(self._jreader.csv(path))
        if isinstance(path, RDD):
            return self._df(self._jreader.csv(path.collect()))
        raise TypeError("path can be only string, list or RDD")

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
        if isinstance(path, str):
            path = [path]
        if isinstance(path, list):
            return self._df(self._jreader.json(path))
        if isinstance(path, RDD):
            return self._df(self._jreader.json(path.collect()))
        raise TypeError("path can be only string, list or RDD")

    def text(self, paths, wholetext=False, lineSep=None,
             pathGlobFilter=None, recursiveFileLookup=None):
        self._set_opts(wholetext=wholetext,
                       lineSep=lineSep,
                       pathGlobFilter=pathGlobFilter,
                       recursiveFileLookup=recursiveFileLookup)
        if isinstance(paths, str):
            paths = [paths]
        if isinstance(paths, list):
            return self._df(self._jreader.text(paths))
        if isinstance(paths, RDD):
            return self._df(self._jreader.text(paths.collect()))
        raise TypeError("paths can be only string, list or RDD")

