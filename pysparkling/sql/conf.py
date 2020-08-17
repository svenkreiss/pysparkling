class RuntimeConfig(object):
    def __init__(self, jconf=None):
        self._conf = {}

    def set(self, key, value):
        self._conf[key] = value

    def get(self, key, default):
        return self._conf.get(key, default)

    def unset(self, key):
        del self._conf[key]

    def isModifiable(self, key):
        raise NotImplementedError("pysparkling does not support yet this feature")
