from collections import namedtuple

import pytest

from pysparkling._config import InvalidVersionFound, Version, config

ConstructorTest = namedtuple('ConstructorTest', 'pass_in major minor patch')
ComparisonTest = namedtuple('ComparisonTest', 'v1 operation v2 result')

constructor_tests_to_run = [
    ConstructorTest(pass_in=['1.2.3'], major=1, minor=2, patch=3),
    ConstructorTest(pass_in=[1, 2, 3], major=1, minor=2, patch=3),
    ConstructorTest(pass_in=['1', 2, 3], major=1, minor=2, patch=3),
    ConstructorTest(pass_in=[], major=0, minor=0, patch=0),
    ConstructorTest(pass_in=[None], major=0, minor=0, patch=0),
]

comparison_tests_to_run = [
    ComparisonTest(v1=Version('1'), operation='==', v2=Version('1.0'), result=True),
    ComparisonTest(v1='1', operation='==', v2=Version('1.0'), result=True),
    ComparisonTest(v1=Version('1'), operation='==', v2='1.0', result=True),
    ComparisonTest(v1=Version('1'), operation='<=', v2=Version('1.0.1'), result=True),
    ComparisonTest(v1=Version('1'), operation='<', v2=Version('1.0.1'), result=True),
    ComparisonTest(v1=Version('1'), operation='>=', v2=Version('1.0.1'), result=False),
    ComparisonTest(v1=Version('1'), operation='>', v2=Version('1.0.1'), result=False),
    ComparisonTest(v1=Version('1'), operation='!=', v2=Version('1.0.1'), result=True),
    ComparisonTest(v1=Version('1'), operation='!=', v2=Version('1.0.0'), result=False),
    ComparisonTest(v1=None, operation='==', v2=Version('0.0.0'), result=True),
]


@pytest.mark.parametrize('pass_in, major, minor, patch', constructor_tests_to_run)
def test_constructor(pass_in, major, minor, patch):
    v = Version(*pass_in)

    assert v.major == major
    assert v.minor == minor
    assert v.patch == patch


def test_wrong_version_constructor():
    with pytest.raises(InvalidVersionFound):
        Version('1.0.dev')


def test_repr():
    assert str(Version('1.2.3')) == "Version('1.2.3')"


@pytest.mark.parametrize('v1, operation, v2, result', comparison_tests_to_run)
def test_comparison(v1, v2, operation, result):
    if operation == '==':
        assert (v1 == v2) == result
    elif operation == '!=':
        assert (v1 != v2) == result
    elif operation == '<=':
        assert (v1 <= v2) == result
    elif operation == '>=':
        assert (v1 >= v2) == result
    elif operation == '<':
        assert (v1 < v2) == result
    elif operation == '>':
        assert (v1 > v2) == result


def test_set_get_version():
    assert config.spark_version is None

    config.spark_version = '1.2'
    assert config.spark_version == Version(1, 2)

    del config.spark_version
    assert config.spark_version is None

    config.spark_version = '1.2'
    config.spark_version = None
    assert config.spark_version is None

    config.spark_version = '1.2'
    config.spark_version = '1.3'
    assert config.spark_version == Version(1, 3)

    config.spark_version = '1.2'
    config.spark_version = Version('1.3')
    assert config.spark_version == Version(1, 3)


def test_in_set():
    """This tests the hash() method as well."""

    sut = {
        Version(1, 2, 3),
        Version('1.2.3'),
        Version('2.3.1'),
    }

    assert len(sut) == 2
