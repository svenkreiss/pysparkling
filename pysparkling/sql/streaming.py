class DataStreamWriter(object):
    """
    Interface used to write a streaming :class:`DataFrame` to external storage systems
    (e.g. file systems, key-value stores, etc). Use :func:`DataFrame.writeStream`
    to access this.

    .. note:: Evolving.

    .. versionadded:: 2.0
    """

    def __init__(self, df):
        self._df = df
        self._spark = df.sql_ctx
        self._jwrite = df._jdf.writeStream()

    def _sq(self, jsq):
        from pyspark.sql.streaming import StreamingQuery
        return StreamingQuery(jsq)

    def outputMode(self, outputMode):
        """Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.

        Options include:

        * `append`:Only the new rows in the streaming DataFrame/Dataset will be written to
           the sink
        * `complete`:All the rows in the streaming DataFrame/Dataset will be written to the sink
           every time these is some updates
        * `update`:only the rows that were updated in the streaming DataFrame/Dataset will be
           written to the sink every time there are some updates. If the query doesn't contain
           aggregations, it will be equivalent to `append` mode.

       .. note:: Evolving.

        >>> writer = sdf.writeStream.outputMode('append')
        """
        if not outputMode or type(outputMode) != str or len(outputMode.strip()) == 0:
            raise ValueError('The output mode must be a non-empty string. Got: %s' % outputMode)
        self._jwrite = self._jwrite.outputMode(outputMode)
        return self

    def format(self, source):
        """Specifies the underlying output data source.

        .. note:: Evolving.

        :param source: string, name of the data source, which for now can be 'parquet'.

        >>> writer = sdf.writeStream.format('json')
        """
        self._jwrite = self._jwrite.format(source)
        return self

    def option(self, key, value):
        """Adds an output option for the underlying data source.

        You can set the following option(s) for writing files:
            * ``timeZone``: sets the string that indicates a timezone to be used to format
                timestamps in the JSON/CSV datasources or partition values.
                If it isn't set, it uses the default value, session local timezone.

        .. note:: Evolving.
        """
        self._jwrite = self._jwrite.option(key, to_str(value))
        return self

    def options(self, **options):
        """Adds output options for the underlying data source.

        You can set the following option(s) for writing files:
            * ``timeZone``: sets the string that indicates a timezone to be used to format
                timestamps in the JSON/CSV datasources or partition values.
                If it isn't set, it uses the default value, session local timezone.

       .. note:: Evolving.
        """
        for k in options:
            self._jwrite = self._jwrite.option(k, to_str(options[k]))
        return self

    def partitionBy(self, *cols):
        """Partitions the output by the given columns on the file system.

        If specified, the output is laid out on the file system similar
        to Hive's partitioning scheme.

        .. note:: Evolving.

        :param cols: name of columns

        """
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]
        self._jwrite = self._jwrite.partitionBy(_to_seq(self._spark._sc, cols))
        return self

    def queryName(self, queryName):
        """Specifies the name of the :class:`StreamingQuery` that can be started with
        :func:`start`. This name must be unique among all the currently active queries
        in the associated SparkSession.

        .. note:: Evolving.

        :param queryName: unique name for the query

        >>> writer = sdf.writeStream.queryName('streaming_query')
        """
        if not queryName or type(queryName) != str or len(queryName.strip()) == 0:
            raise ValueError('The queryName must be a non-empty string. Got: %s' % queryName)
        self._jwrite = self._jwrite.queryName(queryName)
        return self

    def trigger(self, processingTime=None, once=None, continuous=None):
        """Set the trigger for the stream query. If this is not set it will run the query as fast
        as possible, which is equivalent to setting the trigger to ``processingTime='0 seconds'``.

        .. note:: Evolving.

        :param processingTime: a processing time interval as a string, e.g. '5 seconds', '1 minute'.
                               Set a trigger that runs a query periodically based on the processing
                               time. Only one trigger can be set.
        :param once: if set to True, set a trigger that processes only one batch of data in a
                     streaming query then terminates the query. Only one trigger can be set.

        >>> # trigger the query for execution every 5 seconds
        >>> writer = sdf.writeStream.trigger(processingTime='5 seconds')
        >>> # trigger the query for just once batch of data
        >>> writer = sdf.writeStream.trigger(once=True)
        >>> # trigger the query for execution every 5 seconds
        >>> writer = sdf.writeStream.trigger(continuous='5 seconds')
        """
        params = [processingTime, once, continuous]

        if params.count(None) == 3:
            raise ValueError('No trigger provided')
        elif params.count(None) < 2:
            raise ValueError('Multiple triggers not allowed.')

        jTrigger = None
        if processingTime is not None:
            if type(processingTime) != str or len(processingTime.strip()) == 0:
                raise ValueError('Value for processingTime must be a non empty string. Got: %s' %
                                 processingTime)
            interval = processingTime.strip()
            jTrigger = self._spark._sc._jvm.org.apache.spark.sql.streaming.Trigger.ProcessingTime(
                interval)

        elif once is not None:
            if once is not True:
                raise ValueError('Value for once must be True. Got: %s' % once)
            jTrigger = self._spark._sc._jvm.org.apache.spark.sql.streaming.Trigger.Once()

        else:
            if type(continuous) != str or len(continuous.strip()) == 0:
                raise ValueError('Value for continuous must be a non empty string. Got: %s' %
                                 continuous)
            interval = continuous.strip()
            jTrigger = self._spark._sc._jvm.org.apache.spark.sql.streaming.Trigger.Continuous(
                interval)

        self._jwrite = self._jwrite.trigger(jTrigger)
        return self

    def foreach(self, f):
        """
        Sets the output of the streaming query to be processed using the provided writer ``f``.
        This is often used to write the output of a streaming query to arbitrary storage systems.
        The processing logic can be specified in two ways.

        #. A **function** that takes a row as input.
            This is a simple way to express your processing logic. Note that this does
            not allow you to deduplicate generated data when failures cause reprocessing of
            some input data. That would require you to specify the processing logic in the next
            way.

        #. An **object** with a ``process`` method and optional ``open`` and ``close`` methods.
            The object can have the following methods.

            * ``open(partition_id, epoch_id)``: *Optional* method that initializes the processing
                (for example, open a connection, start a transaction, etc). Additionally, you can
                use the `partition_id` and `epoch_id` to deduplicate regenerated data
                (discussed later).

            * ``process(row)``: *Non-optional* method that processes each :class:`Row`.

            * ``close(error)``: *Optional* method that finalizes and cleans up (for example,
                close connection, commit transaction, etc.) after all rows have been processed.

            The object will be used by Spark in the following way.

            * A single copy of this object is responsible of all the data generated by a
                single task in a query. In other words, one instance is responsible for
                processing one partition of the data generated in a distributed manner.

            * This object must be serializable because each task will get a fresh
                serialized-deserialized copy of the provided object. Hence, it is strongly
                recommended that any initialization for writing data (e.g. opening a
                connection or starting a transaction) is done after the `open(...)`
                method has been called, which signifies that the task is ready to generate data.

            * The lifecycle of the methods are as follows.

                For each partition with ``partition_id``:

                ... For each batch/epoch of streaming data with ``epoch_id``:

                ....... Method ``open(partitionId, epochId)`` is called.

                ....... If ``open(...)`` returns true, for each row in the partition and
                        batch/epoch, method ``process(row)`` is called.

                ....... Method ``close(errorOrNull)`` is called with error (if any) seen while
                        processing rows.

            Important points to note:

            * The `partitionId` and `epochId` can be used to deduplicate generated data when
                failures cause reprocessing of some input data. This depends on the execution
                mode of the query. If the streaming query is being executed in the micro-batch
                mode, then every partition represented by a unique tuple (partition_id, epoch_id)
                is guaranteed to have the same data. Hence, (partition_id, epoch_id) can be used
                to deduplicate and/or transactionally commit data and achieve exactly-once
                guarantees. However, if the streaming query is being executed in the continuous
                mode, then this guarantee does not hold and therefore should not be used for
                deduplication.

            * The ``close()`` method (if exists) will be called if `open()` method exists and
                returns successfully (irrespective of the return value), except if the Python
                crashes in the middle.

        .. note:: Evolving.

        >>> # Print every row using a function
        >>> def print_row(row):
        ...     print(row)
        ...
        >>> writer = sdf.writeStream.foreach(print_row)
        >>> # Print every row using a object with process() method
        >>> class RowPrinter:
        ...     def open(self, partition_id, epoch_id):
        ...         print("Opened %d, %d" % (partition_id, epoch_id))
        ...         return True
        ...     def process(self, row):
        ...         print(row)
        ...     def close(self, error):
        ...         print("Closed with error: %s" % str(error))
        ...
        >>> writer = sdf.writeStream.foreach(RowPrinter())
        """

        from pyspark.rdd import _wrap_function
        from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
        from pyspark.taskcontext import TaskContext

        if callable(f):
            # The provided object is a callable function that is supposed to be called on each row.
            # Construct a function that takes an iterator and calls the provided function on each
            # row.
            def func_without_process(_, iterator):
                for x in iterator:
                    f(x)
                return iter([])

            func = func_without_process

        else:
            # The provided object is not a callable function. Then it is expected to have a
            # 'process(row)' method, and optional 'open(partition_id, epoch_id)' and
            # 'close(error)' methods.

            if not hasattr(f, 'process'):
                raise Exception("Provided object does not have a 'process' method")

            if not callable(getattr(f, 'process')):
                raise Exception("Attribute 'process' in provided object is not callable")

            def doesMethodExist(method_name):
                exists = hasattr(f, method_name)
                if exists and not callable(getattr(f, method_name)):
                    raise Exception(
                        "Attribute '%s' in provided object is not callable" % method_name)
                return exists

            open_exists = doesMethodExist('open')
            close_exists = doesMethodExist('close')

            def func_with_open_process_close(partition_id, iterator):
                epoch_id = TaskContext.get().getLocalProperty('streaming.sql.batchId')
                if epoch_id:
                    epoch_id = int(epoch_id)
                else:
                    raise Exception("Could not get batch id from TaskContext")

                # Check if the data should be processed
                should_process = True
                if open_exists:
                    should_process = f.open(partition_id, epoch_id)

                error = None

                try:
                    if should_process:
                        for x in iterator:
                            f.process(x)
                except Exception as ex:
                    error = ex
                finally:
                    if close_exists:
                        f.close(error)
                    if error:
                        raise error

                return iter([])

            func = func_with_open_process_close

        serializer = AutoBatchedSerializer(PickleSerializer())
        wrapped_func = _wrap_function(self._spark._sc, func, serializer, serializer)
        jForeachWriter = \
            self._spark._sc._jvm.org.apache.spark.sql.execution.python.PythonForeachWriter(
                wrapped_func, self._df._jdf.schema())
        self._jwrite.foreach(jForeachWriter)
        return self

    def foreachBatch(self, func):
        """
        Sets the output of the streaming query to be processed using the provided
        function. This is supported only the in the micro-batch execution modes (that is, when the
        trigger is not continuous). In every micro-batch, the provided function will be called in
        every micro-batch with (i) the output rows as a DataFrame and (ii) the batch identifier.
        The batchId can be used deduplicate and transactionally write the output
        (that is, the provided Dataset) to external systems. The output DataFrame is guaranteed
        to exactly same for the same batchId (assuming all operations are deterministic in the
        query).

        .. note:: Evolving.

        >>> def func(batch_df, batch_id):
        ...     batch_df.collect()
        ...
        >>> writer = sdf.writeStream.foreach(func)
        """

        from pyspark.java_gateway import ensure_callback_server_started
        gw = self._spark._sc._gateway
        java_import(gw.jvm, "org.apache.spark.sql.execution.streaming.sources.*")

        wrapped_func = ForeachBatchFunction(self._spark, func)
        gw.jvm.PythonForeachBatchHelper.callForeachBatch(self._jwrite, wrapped_func)
        ensure_callback_server_started(gw)
        return self

    def start(self, path=None, format=None, outputMode=None, partitionBy=None, queryName=None,
              **options):
        """Streams the contents of the :class:`DataFrame` to a data source.

        The data source is specified by the ``format`` and a set of ``options``.
        If ``format`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        .. note:: Evolving.

        :param path: the path in a Hadoop supported file system
        :param format: the format used to save
        :param outputMode: specifies how data of a streaming DataFrame/Dataset is written to a
                           streaming sink.

            * `append`:Only the new rows in the streaming DataFrame/Dataset will be written to the
              sink
            * `complete`:All the rows in the streaming DataFrame/Dataset will be written to the sink
               every time these is some updates
            * `update`:only the rows that were updated in the streaming DataFrame/Dataset will be
              written to the sink every time there are some updates. If the query doesn't contain
              aggregations, it will be equivalent to `append` mode.
        :param partitionBy: names of partitioning columns
        :param queryName: unique name for the query
        :param options: All other string options. You may want to provide a `checkpointLocation`
                        for most streams, however it is not required for a `memory` stream.

        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()
        >>> sq.isActive
        True
        >>> sq.name
        u'this_query'
        >>> sq.stop()
        >>> sq.isActive
        False
        >>> sq = sdf.writeStream.trigger(processingTime='5 seconds').start(
        ...     queryName='that_query', outputMode="append", format='memory')
        >>> sq.name
        u'that_query'
        >>> sq.isActive
        True
        >>> sq.stop()
        """
        self.options(**options)
        if outputMode is not None:
            self.outputMode(outputMode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        if queryName is not None:
            self.queryName(queryName)
        if path is None:
            return self._sq(self._jwrite.start())
        else:
            return self._sq(self._jwrite.start(path))

