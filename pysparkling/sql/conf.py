import sys

if sys.version_info[0] >= 3:
    basestring = str

_sentinel = object()


class RuntimeConfig(object):
    def __init__(self, jconf=None):
        self._conf = {}

    def set(self, key, value):
        self._conf[key]= value

    def get(self, key, default=_sentinel):
        self._checkType(key, "key")
        if default is _sentinel:
            return self._conf.get(key)
        else:
            if default is not None:
                self._checkType(default, "default")
            return self._conf.get(key, default)

    def unset(self, key):
        del self._conf[key]

    def _checkType(self, obj, identifier):
        if not isinstance(obj, basestring):
            raise TypeError("expected %s '%s' to be a string (was '%s')" %
                            (identifier, obj, type(obj).__name__))

    def isModifiable(self, key):
        # todo: isModifiable is not implemented
        return self._conf.isModifiable(key)

