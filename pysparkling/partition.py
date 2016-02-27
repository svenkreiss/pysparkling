from __future__ import absolute_import

import logging

log = logging.getLogger(__name__)


class Partition(object):
    def __init__(self, x, idx=None):
        self.index = idx
        self._x = x

    def x(self):
        if not isinstance(self._x, list):
            self._x = list(self._x)
        return self._x

    def hashCode(self):
        return self.index

    def __getstate__(self):
        return {
            'index': self.index,
            '_x': self.x(),
        }
