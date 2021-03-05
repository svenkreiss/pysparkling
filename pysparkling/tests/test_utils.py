import pytest

import pysparkling
from pysparkling.utils import NotSupportedByThisSparkVersion, since, until


@since('2.3.2')
def since_232():
    return 'ok'


@since('2.4')
def since_24():
    return 'ok'


@until('2.4')
def until_24():
    return 'ok'


@until('2.3.2')
def until_232():
    return 'ok'


@since('2.3')
@until('2.4')
def since_23_until_24():
    return 'ok'


@since('2.4')
class SomeNewFeatureSince:
    def a(self):
        return 'ok'


@until('2.4')
class SomeNewFeatureUntil:
    def a(self):
        return 'ok'


def test_happy_flow():
    pysparkling.config.spark_version = '2.0'
    assert until_24() == 'ok'


@pytest.mark.parametrize('spark_version_to_set, method, should_raise', [
    ('2.3.1', since_232, True),
    ('2.3.2', since_232, False),
    ('2.3.3', since_232, False),

    ('2.3.3', since_24, True),
    ('2.4', since_24, False),

    ('2.3.2', SomeNewFeatureSince().a, True),
    ('2.4.1', SomeNewFeatureSince().a, False),

    # The following I only need to check once (not in since/until twice).
    ('2.2', since_23_until_24, True),
    ('2.3', since_23_until_24, False),
    ('2.3.1', since_23_until_24, False),
    ('2.4', since_23_until_24, True),
])
def test_since(spark_version_to_set, method, should_raise):
    pysparkling.config.spark_version = spark_version_to_set
    if should_raise:
        with pytest.raises(NotSupportedByThisSparkVersion):
            method()
    else:
        assert method() == 'ok'


@pytest.mark.parametrize('spark_version_to_set, method, should_raise', [
    ('2.3.1', until_232, False),
    ('2.3.2', until_232, True),
    ('2.3.3', until_232, True),

    ('2.3.3', until_24, False),
    ('2.4', until_24, True),

    ('2.3.2', SomeNewFeatureUntil().a, False),
    ('2.4.1', SomeNewFeatureUntil().a, True),
])
def test_until(spark_version_to_set, method, should_raise):
    pysparkling.config.spark_version = spark_version_to_set
    if should_raise:
        with pytest.raises(NotSupportedByThisSparkVersion):
            method()
    else:
        assert method() == 'ok'
