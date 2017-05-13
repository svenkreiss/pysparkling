from __future__ import absolute_import, print_function

import logging
import time

from .dstream import DStream
from .filestream import (FileStream, FileTextStreamDeserializer,
                         FileBinaryStreamDeserializer)
from .queuestream import QueueStream, QueueStreamDeserializer

try:
    from .tcpstream import TCPTextStream, TCPBinaryStream, TCPDeserializer
except ImportError:
    TCPTextStream = False
    TCPBinaryStream = False
    TCPDeserializer = False

try:
    import tornado
    from tornado.ioloop import IOLoop, PeriodicCallback
    from tornado.queues import Queue
except ImportError:
    tornado = False
    IOLoop = False
    Queue = False
    PeriodicCallback = False

log = logging.getLogger(__name__)


class StreamingContext(object):
    """Stream processing.

    :param pysparkling.Context sparkContext: A pysparkling.Context.
    :param float batchDuration: Duration in seconds per batch.
    """

    _activeContext = None

    def __init__(self, sparkContext, batchDuration=None):
        self._context = sparkContext
        self.batch_duration = batchDuration if batchDuration is not None else 1
        self._dstreams = []
        self._pcb = None
        self._on_stop_cb = []

    @property
    def sparkContext(self):
        """Return context of this StreamingContext."""
        return self._context

    def _add_dstream(self, dstream):
        self._dstreams.append(dstream)

    def awaitTermination(self, timeout=None):
        """Wait for context to stop.

        :param float timeout: in seconds
        """

        if timeout is not None:
            IOLoop.current().call_later(timeout, self.stop)

        IOLoop.current().start()
        IOLoop.clear_current()

    def awaitTerminationOrTimeout(self, timeout):
        """Provided for compatibility. Same as ``awaitTermination()`` here."""
        return self.awaitTermination(timeout)

    def binaryRecordsStream(self, directory, recordLength=None,
                            process_all=False):
        """Monitor a directory and process all binary files.

        File names starting with ``.`` are ignored.

        :param string directory: a path
        :param recordLength: None, int or struct format string
        :param bool process_all: whether to process pre-existing files
        :rtype: DStream

        .. warning::
            Only ``int`` ``recordLength`` are supported in PySpark API.
            The ``process_all`` parameter does not exist in the PySpark API.
        """
        deserializer = FileBinaryStreamDeserializer(self._context,
                                                    recordLength)
        file_stream = FileStream(directory, process_all)
        self._on_stop_cb.append(file_stream.stop)
        return DStream(file_stream, self, deserializer)

    def queueStream(self, rdds, oneAtATime=True, default=None):
        """Create stream iterable over RDDs.

        :param rdds: Iterable over RDDs or lists.
        :param oneAtATime: Process one at a time or all.
        :param default: If no more RDDs in ``rdds``, return this. Can be None.
        :rtype: DStream


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> (
        ...     ssc
        ...     .queueStream([[4], [2], [7]])
        ...     .foreachRDD(lambda rdd: print(rdd.collect()))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.35)
        [4]
        [2]
        [7]


        Example testing the default value:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> (
        ...     ssc
        ...     .queueStream([[4], [2]], default=['placeholder'])
        ...     .foreachRDD(lambda rdd: print(rdd.collect()))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.35)
        [4]
        [2]
        ['placeholder']
        """
        deserializer = QueueStreamDeserializer(self._context)
        if default is not None:
            default = deserializer(default)

        if Queue is False:
            log.error('Run "pip install tornado" to install tornado.')

        q = Queue()
        for i in rdds:
            q.put(i)

        qstream = QueueStream(q, oneAtATime, default)
        return DStream(qstream, self, deserializer)

    def remember(self, duration):
        """Provided for compatibility, but does nothing here."""
        pass

    def socketBinaryStream(self, hostname, port, length):
        """Create a TCP socket server for binary input.

        .. warning::
            This is not part of the PySpark API.

        :param string hostname: Hostname of TCP server.
        :param int port: Port of TCP server.
        :param length:
            Message length. Length in bytes or a format string for
            ``struct.unpack()``.

            For variable length messages where the message length is sent right
            before the message itself, ``length`` is
            a format string that can be passed to ``struct.unpack()``.
            For example, use ``length='<I'`` for a little-endian
            (standard on x86) 32-bit unsigned int.
        :rtype: DStream
        """
        deserializer = TCPDeserializer(self._context)
        tcp_binary_stream = TCPBinaryStream(length)
        tcp_binary_stream.listen(port, hostname)
        self._on_stop_cb.append(tcp_binary_stream.stop)
        return DStream(tcp_binary_stream, self, deserializer)

    def socketTextStream(self, hostname, port):
        """Create a TCP socket server.

        :param string hostname: Hostname of TCP server.
        :param int port: Port of TCP server.
        :rtype: DStream
        """
        deserializer = TCPDeserializer(self._context)
        tcp_text_stream = TCPTextStream()
        tcp_text_stream.listen(port, hostname)
        self._on_stop_cb.append(tcp_text_stream.stop)
        return DStream(tcp_text_stream, self, deserializer)

    def start(self):
        """Start processing streams."""

        def cb():
            time_ = time.time()
            log.debug('Step {}'.format(time_))

            # run a step on all streams
            for d in self._dstreams:
                d._step(time_)

        self._pcb = PeriodicCallback(cb, self.batch_duration * 1000.0)
        self._pcb.start()
        self._on_stop_cb.append(self._pcb.stop)
        StreamingContext._activeContext = self

    def stop(self, stopSparkContext=True, stopGraceFully=False):
        """Stop processing streams.

        :param stopSparkContext: stop the SparkContext (NOT IMPLEMENTED)
        :param stopGracefully: stop gracefully (NOT IMPLEMENTED)
        """
        while self._on_stop_cb:
            cb = self._on_stop_cb.pop()
            log.debug('calling on_stop_cb {}'.format(cb))
            cb()

        IOLoop.current().stop()

        StreamingContext._activeContext = None

    def textFileStream(self, directory, process_all=False):
        """Monitor a directory and process all text files.

        File names starting with ``.`` are ignored.

        :param string directory: a path
        :param bool process_all: whether to process pre-existing files
        :rtype: DStream

        .. warning::
            The ``process_all`` parameter does not exist in the PySpark API.
        """
        deserializer = FileTextStreamDeserializer(self._context)
        file_stream = FileStream(directory, process_all)
        self._on_stop_cb.append(file_stream.stop)
        return DStream(file_stream, self, deserializer)

    #: Alias of :func:`.textFileStream`.
    fileTextStream = textFileStream

    #: Alias of :func:`.binaryRecordsStream`.
    fileBinaryStream = binaryRecordsStream
