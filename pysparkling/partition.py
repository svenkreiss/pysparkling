from __future__ import absolute_import

import logging
import itertools

log = logging.getLogger(__name__)


class Partition(object):
    def __init__(self, x, idx=None):
        self.index = idx
        self._x = x

    def x(self):
        self._x, r = itertools.tee(self._x)
        return r

    def hashCode(self):
        return self.index

    def __getstate__(self):
        return {
            'index': self.index,
            '_x': list(self.x())
        }
