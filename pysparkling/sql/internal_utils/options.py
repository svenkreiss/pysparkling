class Options(dict):
    """
    A case insensitive dict, which can be initialized from multiple dicts
    and whose values can be access through attr syntax

    It also stores "false" and "true" strings as Boolean

    e.g.:

    >>> default_options = dict(sep=",", samplingRatio=None)
    >>> requested_options = dict(Sep="|")
    >>> o=Options({"format": "json", "lineSep": ","}, Format="csv")
    >>> o.format, o.linesep
    ('csv', ',')
    >>> o.UndefinedSetting
    Traceback (most recent call last):
    ...
    KeyError: 'undefinedsetting'
    """

    def __init__(self, *args, **kwargs):
        d = {
            key.lower(): value
            for arg in args
            if arg is not None
            for key, value in arg.items()
        }
        d.update({
            key.lower(): value
            for key, value in kwargs.items()
        })
        super().__init__(d)

    def setdefault(self, k, default=None):
        return super().setdefault(k.lower(), default)

    @staticmethod
    def fromkeys(seq, value=None):
        return Options({k.lower(): value for k in seq})

    def __getitem__(self, k):
        return super().__getitem__(k.lower())

    def __setitem__(self, k, v):
        if isinstance(v, str) and v.lower() in ("true", "false"):
            v = (v.lower() == "true")
        super().__setitem__(k.lower(), v)

    def __delitem__(self, k):
        super().__delitem__(k.lower())

    def get(self, k, *args, **kwargs):
        return super().get(k.lower(), *args, **kwargs)

    def __contains__(self, o):
        if not isinstance(o, str):
            return False
        return super().__contains__(o.lower())

    def __getattr__(self, item):
        if not item.startswith("_"):
            return self[item.lower()]
        return getattr(super(), item)
