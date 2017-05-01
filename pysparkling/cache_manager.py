"""Manages caches of calculated partitions."""

from __future__ import (division, absolute_import, print_function,
                        unicode_literals)

import logging
import pickle
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
    singleton__ = None

    @staticmethod
    def singleton(max_mem=1.0,
                  serializer=None, deserializer=None,
                  checksum=None):
        if CacheManager.singleton__ is None:
            CacheManager.singleton__ = CacheManager(max_mem,
                                                    serializer, deserializer,
                                                    checksum)
        return CacheManager.singleton__

    def __init__(self,
                 max_mem=1.0,
                 serializer=None, deserializer=None,
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
        """clone contains

        :param filter_id:
            A function returning true for ids that should be returned.

        :returns:
            A new CacheManager with the entries that contain partial_ident
            in the ident.
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
