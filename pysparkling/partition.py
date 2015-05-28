import itertools


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


class PersistedPartition(Partition):
    def __init__(self, x, idx=None, storageLevel=None):
        Partition.__init__(self, x, idx)
        self.cache_x = None
        self.storageLevel = storageLevel

    def x(self):
        if self.cache_x:
            self.cache_x, r = itertools.tee(self.cache_x, 2)
            return r
        return Partition.x(self)

    def set_cache_x(self, x):
        self.cache_x = iter(list(x))

    def __getstate__(self):
        d = {
            'cache_x': list(self.x()) if self.cache_x is not None else None,
            'storageLevel': self.storageLevel,
        }
        d.update(Partition.__getstate__(self))
        return d
