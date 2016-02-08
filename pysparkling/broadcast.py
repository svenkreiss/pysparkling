
__all__ = ['Broadcast']


class Broadcast(object):
    """
    A broadcast variable created with ``b = sc.broadcast(0)``.
    Access its value through ``b.value``.

    Examples:

    >>> from pysparkling import Context
    >>> sc = Context()
    >>> b = sc.broadcast([1, 2, 3, 4, 5])
    >>> b.value
    [1, 2, 3, 4, 5]
    >>> sc.parallelize([0, 0]).flatMap(lambda x: b.value).collect()
    [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
    >>> b.value += [1]
    Traceback (most recent call last):
    ...
    AttributeError: can't set attribute
    """
    def __init__(self, sc=None, value=None):
        self._value = value

    @property
    def value(self):
        """Returs the broadcasted value."""
        return self._value


if __name__ == "__main__":
    """
    Execute doctests with

    $ python -m pysparkling.broadcast -v
    """
    import doctest
    failure_count, _ = doctest.testmod()
    if failure_count:
        exit(-1)
