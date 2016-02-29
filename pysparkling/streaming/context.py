from __future__ import absolute_import

import time
import logging

from ..rdd import RDD
from .dstream import DStream
from .queuestream import QueueStream
from .tcpstream import TCPStream

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
    _activeContext = None

    def __init__(self, sparkContext, batchDuration=None):
        """Stream processing.

        :param sparkContext:
            A pysparkling.Context.

        :param batchDuration:
            Duration in seconds per batch.
        """
        self._context = sparkContext
        self.batch_duration = batchDuration if batchDuration is not None else 1
        self._dstreams = []
        self._pcb = None

    @property
    def sparkContext(self):
        """Return context of this StreamingContext."""
        return self._context

    def _add_dstream(self, dstream):
        self._dstreams.append(dstream)

    def awaitTermination(self, timeout=None):
        """Wait for context to stop.

        :param timeout:
            in seconds
        """

        if timeout is not None:
            IOLoop.current().call_later(timeout, IOLoop.current().stop)

        IOLoop.current().start()

    def queueStream(self, rdds, oneAtATime=True, default=None):
        """Create stream iterable over RDDs.

        :param rdds:
            Iterable over RDDs.
        :param oneAtATime:
            Process one at a time or all.
        :param default:
            If no more RDDs in ``rdds``, return this RDD.
        """
        if default is not None and not isinstance(default, RDD):
            default = self._context.parallelize(default)

        if Queue is False:
            log.error('Run "pip install tornado" to install tornado.')

        q = Queue()
        for i in rdds:
            if isinstance(i, RDD):
                q.put(i)
            else:
                q.put(self._context.parallelize(i))

        qstream = QueueStream(q, oneAtATime, default)
        return DStream(qstream, self)

    def start(self):
        """Start processing streams."""

        def cb():
            time_ = time.time()
            log.debug('Step {}'.format(time_))

            # run a step on all streams
            for d in self._dstreams:
                d._step(time_)

            # run function of all streams
            for d in self._dstreams:
                d._apply(time_)

        self._pcb = PeriodicCallback(cb, self.batch_duration*1000.0)
        self._pcb.start()
        StreamingContext._activeContext = self

    def stop(self, stopSparkContext=True, stopGraceFully=False):
        """Stop processing streams.

        :param stopSparkContext:
            stop the SparkContext (NOT IMPLEMENTED)
        :param stopGracefully:
            stop gracefully (NOT IMPLEMENTED)
        """
        IOLoop.current().stop()
        StreamingContext._activeContext = None

    def textFileStream(self, directory):
        """Creates an input stream that monitors this directory. File names
        starting with ``.`` are ignored."""

    def socketTextStream(self, hostname, port):
        """Create a socket server.

        :param hostname:
            hostname of TCP server.

        :param port:
            Port of TCP server.
        """
        return DStream(TCPStream(hostname, port), self)
