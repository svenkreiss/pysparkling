"""Manages caches of calculated partitions."""

from __future__ import (division, absolute_import, print_function,
                        unicode_literals)

import logging
import pickle
import time
import zlib

log = logging.getLogger(__name__)


class CacheManager(object):
    """cache manager

    When mem_obj or disk_location are None, it means the object does not
    exist in memory or on disk. The other variables might be set though.

    :param max_mem: Memory in GB to keep in memory before spilling to disk.
    :param serializer: Use to serialize cache objects.
    :param deserializer: Use to deserialize cache objects.
    :param checksum: Function returning a checksum.
    """

    def __init__(self, max_mem=1.0, serializer=None, deserializer=None,
                 checksum=None):
        self.max_mem = max_mem
        self.serializer = serializer if serializer else pickle.dumps
        self.deserializer = deserializer if deserializer else pickle.loads
        self.checksum = checksum if checksum else zlib.crc32

        self.clear()

    def incr_cache_cnt(self):
        self.cache_cnt += 1
        return self.cache_cnt

    def add(self, ident, obj, storageLevel=None):
        self.cache_obj[ident] = {
            'id': self.incr_cache_cnt(),
            'storageLevel': storageLevel,
            'mem_size': None,
            'mem_obj': obj,
            'mem_ser': None,
            'mem_ser_size': None,
            'disk_size': None,
            'disk_location': None,
            'checksum': None,
        }
        log.debug('Added {0} to cache.'.format(ident))

    def get(self, ident):
        if ident not in self.cache_obj:
            log.debug('{0} not found in cache.'.format(ident))
            return None

        log.debug('Returning {0} from cache.'.format(ident))
        return self.cache_obj[ident]['mem_obj']

    def has(self, ident):
        return (
            ident in self.cache_obj and (
                self.cache_obj[ident]['mem_obj'] is not None or
                self.cache_obj[ident]['disk_location'] is not None
            )
        )

    def get_not_in(self, idents):
        """get entries not given in idents

        :param idents: A list of cache ids (or idents).
        :returns: All cache entries that are not in the given list.
        """
        return {i: c
                for i, c in self.cache_obj.items()
                if i not in idents}

    def join(self, cache_objects):
        """join

        :param cache_objects:
            Objects obtained with :func:`CacheManager.get_not_in()`.
        """
        self.cache_obj.update(cache_objects)

    def stored_idents(self):
        return [k
                for k, v in self.cache_obj.items()
                if (v['mem_obj'] is not None or
                    v['disk_location'] is not None)]

    def clone_contains(self, filter_id):
        """Clone the cache manager and add a subset of the cache to it.

        :param filter_id:
            A function returning true for ids that should be returned.

        :rtype: CacheManager
        """
        cm = CacheManager(self.max_mem,
                          self.serializer, self.deserializer,
                          self.checksum)
        cm.cache_obj = {i: c
                        for i, c in self.cache_obj.items()
                        if filter_id(i)}
        return cm

    def delete(self, ident):
        if ident not in self.cache_obj:
            return False

        del self.cache_obj[ident]
        return True

    def clear(self):
        """empties the entire cache"""
        self.cache_obj = {}
        self.cache_cnt = 0
        self.cache_mem_size = 0.0
        self.cache_disk_size = 0.0


class TimedCacheManager(CacheManager):
    """Cache manager with a timeout.

    Assigns a timeout to each item that is stored. Timed out entries are only
    removed when an :func:`.add` is called (or :func:`gc` is called
    explicitly).

    :param max_mem: Memory in GB to keep in memory before spilling to disk.
    :param serializer: Use to serialize cache objects.
    :param deserializer: Use to deserialize cache objects.
    :param checksum: Function returning a checksum.
    :param float timeout: timeout duration in seconds
    """
    def __init__(self,
                 max_mem=1.0,
                 serializer=None, deserializer=None,
                 checksum=None, timeout=600.0):
        super(TimedCacheManager, self).__init__(
            max_mem, serializer, deserializer, checksum)

        self.timeout = timeout
        self._time_added = []  # pairs of (id, timestamp); oldest first

    def add(self, ident, obj, storageLevel=None):
        super(TimedCacheManager, self).add(ident, obj, storageLevel)
        self._time_added.append((ident, time.time()))
        self.gc()

    def clone_contains(self, filter_id):
        """Clone the timed cache manager and add a subset of the cache to it.

        :param filter_id:
            A function returning true for ids that should be returned.

        :rtype: TimedCacheManager
        """
        cm = TimedCacheManager(self.max_mem,
                               self.serializer, self.deserializer,
                               self.checksum, self.timeout)
        cm.cache_obj = {i: c
                        for i, c in self.cache_obj.items()
                        if filter_id(i)}
        return cm

    def gc(self):
        """Remove timed out entries."""
        log.debug('Looking for timed out cache entries.')
        threshold_time = time.time() - self.timeout
        while self._time_added:
            ident, timestamp = self._time_added[0]
            if timestamp > threshold_time:
                break
            self.delete(ident)
            del self._time_added[0]
        log.debug('Clear done.')
