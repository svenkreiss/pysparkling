import re
import pytz
from pysparkling.sql.utils import AnalysisException

NO_TIMESTAMP_CONVERSION = object()

JAVA_TIME_FORMAT_TOKENS = re.compile("(([a-zA-Z])\\2*|[^a-zA-Z]+)")

TIME_REGEX = re.compile(
    "^([0-9]+):([0-9]+)?(?::([0-9]+))?(?:\\.([0-9]+))?(Z|[+-][0-9]+(?::(?:[0-9]+)?)?)?$"
)

GMT = pytz.timezone("GMT")


def identity(value, options):
    return value


def cast_from_none(value, from_type, options):
    if value is None:
        return None
    raise AnalysisException(
        "Expected a null value from a field with type {0}, got {1}".format(
            from_type,
            value
        )
    )
