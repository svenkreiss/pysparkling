import logging

log = logging.getLogger(__name__)


class Partition:
    def __init__(self, x, idx=None):
        self.index = idx
        self._x = list(x)

    def x(self):
        return self._x

    def hashCode(self):
        return self.index

    def __getstate__(self):
        return {
            'index': self.index,
            '_x': self.x(),
        }
