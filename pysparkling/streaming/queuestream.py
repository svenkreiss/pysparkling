from __future__ import absolute_import

from ..rdd import EmptyRDD, RDD


class QueueStreamDeserializer:
    def __init__(self, context):
        self.context = context

    def ensure_rdd(self, data):
        if data is None:
            return EmptyRDD(self.context)
        if isinstance(data, RDD):
            return data
        return self.context.parallelize(data)

    def __call__(self, data):
        return self.ensure_rdd(data)


class QueueStream:
    def __init__(self, queue, oneAtATime=True, default=None):
        self.queue = queue
        self.oneAtATime = oneAtATime
        self.default = default

    def get(self):
        q_size = self.queue.qsize()

        if q_size == 0:
            return self.default

        if self.oneAtATime:
            return self.queue.get_nowait()

        return [e for _ in range(q_size) for e in self.queue.get_nowait()]
