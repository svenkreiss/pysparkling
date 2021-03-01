def require_minimum_pandas_version():
    """ Raise an ImportError if Pandas version is < 0.23.2
    """
    minimum_pandas_version = (0, 23, 2)

    # pandas is an optional dependency
    # pylint: disable=import-outside-toplevel
    try:
        import pandas
    except ImportError:
        raise ImportError(f"Pandas >= {minimum_pandas_version} must be installed; however none were found.") from None

    if parse_pandas_version(pandas.__version__) < minimum_pandas_version:
        raise ImportError(
            f"Pandas >= {minimum_pandas_version} must be installed;"
            f" however, your version was {pandas.__version__}."
        )


def parse_pandas_version(version):
    return tuple(int(part) for part in version.split("."))
