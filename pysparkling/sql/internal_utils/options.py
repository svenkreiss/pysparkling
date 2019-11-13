class Options(dict):
    """
    A case insensitive dict, which can be initialized from multiple dicts
    and whose values can be access through attr syntax

    It also stores "false" and "true" strings as Boolean

    e.g.:

    >>> default_options = dict(sep=",", samplingRatio=None)
    >>> requested_options = dict(Sep="|")
    >>> o=Options(default_options, requested_options)
    >>> o
    {'sep': '|', 'samplingratio': None}
    >>> o.SEP, o.samplingratio
    ('|', None)
    >>> o.UndefinedSetting
    Traceback (most recent call last):
    ...
    KeyError: 'undefinedsetting'
    """

    def __init__(self, *args, **kwargs):
        d = {}
        for arg in (*args, kwargs):
            d.update({
                key.lower(): value for key, value in arg.items()
            })
        super().__init__(d)

    def setdefault(self, k, default=None):
        return super().setdefault(k.lower(), default)

    @staticmethod
    def fromkeys(seq):
        return super().fromkeys(k.lower() for k in seq)

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
        return self[item.lower()]
