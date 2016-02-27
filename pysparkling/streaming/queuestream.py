from __future__ import absolute_import


class QueueStream(object):
    def __init__(self, queue, oneAtATime=True, default=None):
        self.queue = queue
        self.oneAtATime = oneAtATime
        self.default = default

    def get(self):
        q_size = self.queue.qsize()

        if q_size == 0:
            return [self.default] if self.default is not None else []

        if self.oneAtATime:
            return [self.queue.get_nowait()]

        return [self.queue.get_nowait() for _ in range(q_size)]
