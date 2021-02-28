from functools import lru_cache, total_ordering
import re
from typing import Union


class InvalidVersionFound(Exception):
    pass


@total_ordering
class Version:
    __slots__ = ['major', 'minor', 'patch']

    VERSION_RE = re.compile(r'^\s*(?P<major>\d+)\s*(?:\.\s*(?P<minor>\d+)\s*(?:\.\s*(?P<patch>\d+)\s*)?)?$')

    def __init__(
        self,
        major: Union[None, str, int] = None,
        minor: Union[None, str, int] = None,
        patch: Union[None, str, int] = None
    ):
        if isinstance(major, str) and not (minor or patch):  # Allow to pass in a str to directly create the object
            major = self._parse_version(major)

        if isinstance(major, Version):
            self.major = major.major
            self.minor = major.minor
            self.patch = major.patch
        else:
            self.major = int(major or 0)
            self.minor = int(minor or 0)
            self.patch = int(patch or 0)

    @staticmethod
    @lru_cache()  # Put this separately so I can cache the method. Small performance gain likely.
    def _parse_version(v):
        if v is None:
            return v

        if isinstance(v, Version):
            return v

        match = Version.VERSION_RE.match(v)
        if not match:
            raise InvalidVersionFound(f"Cannot parse version '{v}' (expected format: '1.2.3').")

        match = match.groupdict()

        return Version(
            major=int(match['major'] or 0),
            minor=int(match['minor'] or 0),
            patch=int(match['patch'] or 0),
        )

    def __eq__(self, other):
        if not isinstance(other, Version):
            other = Version(other)

        return (
            self.major == other.major
            and self.minor == other.minor
            and self.patch == other.patch
        )

    def __lt__(self, other):
        if not isinstance(other, Version):
            other = Version(other)

        return (
            (self.major, self.minor, self.patch)
            < (other.major, other.minor, other.patch)
        )

    def __hash__(self):
        return hash((self.major, self.minor, self.patch))

    def __repr__(self):
        return f"Version('{self.major}.{self.minor}.{self.patch}')"


class SparkVersion:
    def __get__(self, instance, owner):
        if not instance or not hasattr(instance, '_spark_version'):
            return None

        return getattr(instance, '_spark_version')

    def __set__(self, instance, value):
        if not instance:
            return

        if not isinstance(value, Version):
            value = Version(value)

        setattr(instance, '_spark_version', value)


class Configuration:
    """
    Configuration object to store dynamic changes in pysparklings behaviour.
    """

    spark_version = SparkVersion()


# Singleton object, only instantiated in this module
config = Configuration()
