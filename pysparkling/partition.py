from __future__ import absolute_import

import logging
import itertools
from .cache_manager import CacheManager

log = logging.getLogger(__name__)


class Partition(object):
    def __init__(self, x, idx=None):
        self.index = idx
        self.rdd_id = None
        self._x = x

    def x(self):
        self._x, r = itertools.tee(self._x)
        return r

    def hashCode(self):
        return self.index

    def __getstate__(self):
        return {
            'index': self.index,
            'rdd_id': self.rdd_id,
            '_x': list(self.x())
        }


class PersistedPartition(Partition):
    def __init__(self, x, idx=None, storageLevel=None):
        Partition.__init__(self, x, idx)
        self.storageLevel = storageLevel

    def x(self):
        cid = self.cache_id()
        if cid:
            c = CacheManager.singleton().get(self.cache_id())
            if c:
                return iter(c)
        return Partition.x(self)

    def cache_id(self):
        if self.rdd_id is None or self.index is None:
            return None
        return '{0}:{1}'.format(self.rdd_id, self.index)

    def is_cached(self):
        cid = self.cache_id()
        if not cid:
            return False
        return CacheManager.singleton().has(cid)

    def set_cache_x(self, x):
        cid = self.cache_id()
        if not cid:
            log.warn('Could not set cache for RDD {0} and partition {1} '
                     'without cache_id.'.format(self.rdd_id, self.index))
            return
        CacheManager.singleton().add(
            self.cache_id(), list(x), self.storageLevel
        )

    def __getstate__(self):
        d = {
            'storageLevel': self.storageLevel,
        }
        d.update(Partition.__getstate__(self))
        return d
