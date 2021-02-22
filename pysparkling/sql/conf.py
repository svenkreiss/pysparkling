_sentinel = object()


class RuntimeConfig:
    def __init__(self, jconf=None):
        self._conf = {}

    def set(self, key, value):
        self._conf[key] = value

    def get(self, key, default=_sentinel):
        self._checkType(key, "key")
        if default is _sentinel:
            return self._conf.get(key)
        if default is not None:
            self._checkType(default, "default")
        return self._conf.get(key, default)

    def unset(self, key):
        del self._conf[key]

    def _checkType(self, obj, identifier):
        if not isinstance(obj, str):
            raise TypeError("expected %s '%s' to be a string (was '%s')" %
                            (identifier, obj, type(obj).__name__))

    def isModifiable(self, key):
        raise NotImplementedError("pysparkling does not support yet this feature")
