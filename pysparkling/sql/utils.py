from distutils.version import LooseVersion


class CapturedException(Exception):
    pass


class AnalysisException(CapturedException):
    pass


class ParseException(CapturedException):
    pass


class IllegalArgumentException(CapturedException):
    pass


def require_minimum_pandas_version():
    """ Raise an ImportError if Pandas version is < 0.23.2
    """
    minimum_pandas_version = (0, 23, 2)

    # pandas is an optional dependency
    # pylint: disable=C0415
    try:
        import pandas
        have_pandas = True
    except ImportError:
        have_pandas = False

    if not have_pandas:
        raise ImportError(
            "Pandas >= {0} must be installed; however none were found.".format(
                minimum_pandas_version
            )
        )
    if parse_pandas_version(pandas.__version__) < minimum_pandas_version:
        raise ImportError(
            "Pandas >= {0} must be installed; however, your version was {1}.".format(
                minimum_pandas_version,
                pandas.__version__
            )
        )


def parse_pandas_version(version):
    return tuple(int(part) for part in version.split("."))
