class OptionUtils(object):

    def _set_opts(self, schema=None, **options):
        """
        Set named options (filter out those the value is None)
        """
        if schema is not None:
            self.schema(schema)
        for k, v in options.items():
            if v is not None:
                self.option(k, v)


class DataFrameWriter(OptionUtils):
    """
    Interface used to write a :class:`DataFrame` to external storage systems
    (e.g. file systems, key-value stores, etc). Use :func:`DataFrame.write`
    to access this.

    .. versionadded:: 1.4
    """
    def __init__(self, df):
        self._df = df
        self._spark = df.sql_ctx
        self._jwrite = df._jdf.write()

    def _sq(self, jsq):
        from pyspark.sql.streaming import StreamingQuery
        return StreamingQuery(jsq)

    def mode(self, saveMode):
        """Specifies the behavior when data or table already exists.

        Options include:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error` or `errorifexists`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        >>> df.write.mode('append').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        # At the JVM side, the default value of mode is already set to "error".
        # So, if the given saveMode is None, we will not call JVM-side's mode method.
        if saveMode is not None:
            self._jwrite = self._jwrite.mode(saveMode)
        return self

    def format(self, source):
        """Specifies the underlying output data source.

        :param source: string, name of the data source, e.g. 'json', 'parquet'.

        >>> df.write.format('json').save(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self._jwrite = self._jwrite.format(source)
        return self

    def option(self, key, value):
        """Adds an output option for the underlying data source.

        You can set the following option(s) for writing files:
            * ``timeZone``: sets the string that indicates a timezone to be used to format
                timestamps in the JSON/CSV datasources or partition values.
                If it isn't set, it uses the default value, session local timezone.
        """
        self._jwrite = self._jwrite.option(key, to_str(value))
        return self

    def options(self, **options):
        """Adds output options for the underlying data source.

        You can set the following option(s) for writing files:
            * ``timeZone``: sets the string that indicates a timezone to be used to format
                timestamps in the JSON/CSV datasources or partition values.
                If it isn't set, it uses the default value, session local timezone.
        """
        for k in options:
            self._jwrite = self._jwrite.option(k, to_str(options[k]))
        return self

    def partitionBy(self, *cols):
        """Partitions the output by the given columns on the file system.

        If specified, the output is laid out on the file system similar
        to Hive's partitioning scheme.

        :param cols: name of columns

        >>> df.write.partitionBy('year', 'month').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]
        self._jwrite = self._jwrite.partitionBy(_to_seq(self._spark._sc, cols))
        return self

    def bucketBy(self, numBuckets, col, *cols):
        """Buckets the output by the given columns.If specified,
        the output is laid out on the file system similar to Hive's bucketing scheme.

        :param numBuckets: the number of buckets to save
        :param col: a name of a column, or a list of names.
        :param cols: additional names (optional). If `col` is a list it should be empty.

        .. note:: Applicable for file-based data sources in combination with
                  :py:meth:`DataFrameWriter.saveAsTable`.

        >>> (df.write.format('parquet')  # doctest: +SKIP
        ...     .bucketBy(100, 'year', 'month')
        ...     .mode("overwrite")
        ...     .saveAsTable('bucketed_table'))
        """
        if not isinstance(numBuckets, int):
            raise TypeError("numBuckets should be an int, got {0}.".format(type(numBuckets)))

        if isinstance(col, (list, tuple)):
            if cols:
                raise ValueError("col is a {0} but cols are not empty".format(type(col)))

            col, cols = col[0], col[1:]

        if not all(isinstance(c, basestring) for c in cols) or not(isinstance(col, basestring)):
            raise TypeError("all names should be `str`")

        self._jwrite = self._jwrite.bucketBy(numBuckets, col, _to_seq(self._spark._sc, cols))
        return self

    def sortBy(self, col, *cols):
        """Sorts the output in each bucket by the given columns on the file system.

        :param col: a name of a column, or a list of names.
        :param cols: additional names (optional). If `col` is a list it should be empty.

        >>> (df.write.format('parquet')  # doctest: +SKIP
        ...     .bucketBy(100, 'year', 'month')
        ...     .sortBy('day')
        ...     .mode("overwrite")
        ...     .saveAsTable('sorted_bucketed_table'))
        """
        if isinstance(col, (list, tuple)):
            if cols:
                raise ValueError("col is a {0} but cols are not empty".format(type(col)))

            col, cols = col[0], col[1:]

        if not all(isinstance(c, basestring) for c in cols) or not(isinstance(col, basestring)):
            raise TypeError("all names should be `str`")

        self._jwrite = self._jwrite.sortBy(col, _to_seq(self._spark._sc, cols))
        return self

    def save(self, path=None, format=None, mode=None, partitionBy=None, **options):
        """Saves the contents of the :class:`DataFrame` to a data source.

        The data source is specified by the ``format`` and a set of ``options``.
        If ``format`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        :param path: the path in a Hadoop supported file system
        :param format: the format used to save
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        :param partitionBy: names of partitioning columns
        :param options: all other string options

        >>> df.write.mode('append').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode).options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        if path is None:
            self._jwrite.save()
        else:
            self._jwrite.save(path)

    def insertInto(self, tableName, overwrite=False):
        """Inserts the content of the :class:`DataFrame` to the specified table.

        It requires that the schema of the class:`DataFrame` is the same as the
        schema of the table.

        Optionally overwriting any existing data.
        """
        self._jwrite.mode("overwrite" if overwrite else "append").insertInto(tableName)

    def saveAsTable(self, name, format=None, mode=None, partitionBy=None, **options):
        """Saves the content of the :class:`DataFrame` as the specified table.

        In the case the table already exists, behavior of this function depends on the
        save mode, specified by the `mode` function (default to throwing an exception).
        When `mode` is `Overwrite`, the schema of the :class:`DataFrame` does not need to be
        the same as that of the existing table.

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error` or `errorifexists`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        :param name: the table name
        :param format: the format used to save
        :param mode: one of `append`, `overwrite`, `error`, `errorifexists`, `ignore` \
                     (default: error)
        :param partitionBy: names of partitioning columns
        :param options: all other string options
        """
        self.mode(mode).options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        self._jwrite.saveAsTable(name)

    def json(self, path, mode=None, compression=None, dateFormat=None, timestampFormat=None,
             lineSep=None, encoding=None):
        """Saves the content of the :class:`DataFrame` in JSON format
        (`JSON Lines text format or newline-delimited JSON <http://jsonlines.org/>`_) at the
        specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, bzip2, gzip, lz4,
                            snappy and deflate).
        :param dateFormat: sets the string that indicates a date format. Custom date formats
                           follow the formats at ``java.time.format.DateTimeFormatter``. This
                           applies to date type. If None is set, it uses the
                           default value, ``yyyy-MM-dd``.
        :param timestampFormat: sets the string that indicates a timestamp format.
                                Custom date formats follow the formats at
                                ``java.time.format.DateTimeFormatter``.
                                This applies to timestamp type. If None is set, it uses the
                                default value, ``yyyy-MM-dd'T'HH:mm:ss.SSSXXX``.
        :param encoding: specifies encoding (charset) of saved json files. If None is set,
                        the default UTF-8 charset will be used.
        :param lineSep: defines the line separator that should be used for writing. If None is
                        set, it uses the default value, ``\\n``.

        >>> df.write.json(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        self._set_opts(
            compression=compression, dateFormat=dateFormat, timestampFormat=timestampFormat,
            lineSep=lineSep, encoding=encoding)
        self._jwrite.json(path)

    def parquet(self, path, mode=None, partitionBy=None, compression=None):
        """Saves the content of the :class:`DataFrame` in Parquet format at the specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        :param partitionBy: names of partitioning columns
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, uncompressed, snappy, gzip,
                            lzo, brotli, lz4, and zstd). This will override
                            ``spark.sql.parquet.compression.codec``. If None is set, it uses the
                            value specified in ``spark.sql.parquet.compression.codec``.

        >>> df.write.parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._set_opts(compression=compression)
        self._jwrite.parquet(path)

    def text(self, path, compression=None, lineSep=None):
        """Saves the content of the DataFrame in a text file at the specified path.
        The text files will be encoded as UTF-8.

        :param path: the path in any Hadoop supported file system
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, bzip2, gzip, lz4,
                            snappy and deflate).
        :param lineSep: defines the line separator that should be used for writing. If None is
                        set, it uses the default value, ``\\n``.

        The DataFrame must have only one column that is of string type.
        Each row becomes a new line in the output file.
        """
        self._set_opts(compression=compression, lineSep=lineSep)
        self._jwrite.text(path)

    def csv(self, path, mode=None, compression=None, sep=None, quote=None, escape=None,
            header=None, nullValue=None, escapeQuotes=None, quoteAll=None, dateFormat=None,
            timestampFormat=None, ignoreLeadingWhiteSpace=None, ignoreTrailingWhiteSpace=None,
            charToEscapeQuoteEscaping=None, encoding=None, emptyValue=None, lineSep=None):
        r"""Saves the content of the :class:`DataFrame` in CSV format at the specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.

        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, bzip2, gzip, lz4,
                            snappy and deflate).
        :param sep: sets a single character as a separator for each field and value. If None is
                    set, it uses the default value, ``,``.
        :param quote: sets a single character used for escaping quoted values where the
                      separator can be part of the value. If None is set, it uses the default
                      value, ``"``. If an empty string is set, it uses ``u0000`` (null character).
        :param escape: sets a single character used for escaping quotes inside an already
                       quoted value. If None is set, it uses the default value, ``\``
        :param escapeQuotes: a flag indicating whether values containing quotes should always
                             be enclosed in quotes. If None is set, it uses the default value
                             ``true``, escaping all values containing a quote character.
        :param quoteAll: a flag indicating whether all values should always be enclosed in
                          quotes. If None is set, it uses the default value ``false``,
                          only escaping values containing a quote character.
        :param header: writes the names of columns as the first line. If None is set, it uses
                       the default value, ``false``.
        :param nullValue: sets the string representation of a null value. If None is set, it uses
                          the default value, empty string.
        :param dateFormat: sets the string that indicates a date format. Custom date formats
                           follow the formats at ``java.time.format.DateTimeFormatter``. This
                           applies to date type. If None is set, it uses the
                           default value, ``yyyy-MM-dd``.
        :param timestampFormat: sets the string that indicates a timestamp format.
                                Custom date formats follow the formats at
                                ``java.time.format.DateTimeFormatter``.
                                This applies to timestamp type. If None is set, it uses the
                                default value, ``yyyy-MM-dd'T'HH:mm:ss.SSSXXX``.
        :param ignoreLeadingWhiteSpace: a flag indicating whether or not leading whitespaces from
                                        values being written should be skipped. If None is set, it
                                        uses the default value, ``true``.
        :param ignoreTrailingWhiteSpace: a flag indicating whether or not trailing whitespaces from
                                         values being written should be skipped. If None is set, it
                                         uses the default value, ``true``.
        :param charToEscapeQuoteEscaping: sets a single character used for escaping the escape for
                                          the quote character. If None is set, the default value is
                                          escape character when escape and quote characters are
                                          different, ``\0`` otherwise..
        :param encoding: sets the encoding (charset) of saved csv files. If None is set,
                         the default UTF-8 charset will be used.
        :param emptyValue: sets the string representation of an empty value. If None is set, it uses
                           the default value, ``""``.
        :param lineSep: defines the line separator that should be used for writing. If None is
                        set, it uses the default value, ``\\n``. Maximum length is 1 character.

        >>> df.write.csv(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        self._set_opts(compression=compression, sep=sep, quote=quote, escape=escape, header=header,
                       nullValue=nullValue, escapeQuotes=escapeQuotes, quoteAll=quoteAll,
                       dateFormat=dateFormat, timestampFormat=timestampFormat,
                       ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
                       ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace,
                       charToEscapeQuoteEscaping=charToEscapeQuoteEscaping,
                       encoding=encoding, emptyValue=emptyValue, lineSep=lineSep)
        self._jwrite.csv(path)

    def orc(self, path, mode=None, partitionBy=None, compression=None):
        """Saves the content of the :class:`DataFrame` in ORC format at the specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        :param partitionBy: names of partitioning columns
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, snappy, zlib, and lzo).
                            This will override ``orc.compress`` and
                            ``spark.sql.orc.compression.codec``. If None is set, it uses the value
                            specified in ``spark.sql.orc.compression.codec``.

        >>> orc_df = spark.read.orc('python/test_support/sql/orc_partitioned')
        >>> orc_df.write.orc(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._set_opts(compression=compression)
        self._jwrite.orc(path)

    def jdbc(self, url, table, mode=None, properties=None):
        """Saves the content of the :class:`DataFrame` to an external database table via JDBC.

        .. note:: Don't create too many partitions in parallel on a large cluster;
            otherwise Spark might crash your external database systems.

        :param url: a JDBC URL of the form ``jdbc:subprotocol:subname``
        :param table: Name of the table in the external database.
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        :param properties: a dictionary of JDBC database connection arguments. Normally at
                           least properties "user" and "password" with their corresponding values.
                           For example { 'user' : 'SYSTEM', 'password' : 'mypassword' }
        """
        if properties is None:
            properties = dict()
        jprop = JavaClass("java.util.Properties", self._spark._sc._gateway._gateway_client)()
        for k in properties:
            jprop.setProperty(k, properties[k])
        self.mode(mode)._jwrite.jdbc(url, table, jprop)

