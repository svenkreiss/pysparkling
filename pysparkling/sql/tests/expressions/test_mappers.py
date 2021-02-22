from unittest import TestCase

from pysparkling.utils import MonotonicallyIncreasingIDGenerator


class MonotonicallyIncreasingIDGeneratorTests(TestCase):
    def test_init_ok(self):
        sut = MonotonicallyIncreasingIDGenerator(0)
        self.assertEqual(sut.value, -1)  # Shouldn't we throw an error here?

        sut = MonotonicallyIncreasingIDGenerator(1)
        self.assertEqual(sut.value, 8589934592 - 1)  # I do it this way so I can easily find/replace the value

        sut = MonotonicallyIncreasingIDGenerator(2)
        self.assertEqual(sut.value, 2*8589934592 - 1)

    def test_next_value_ok(self):
        sut = MonotonicallyIncreasingIDGenerator(1)
        self.assertEqual(next(sut), 8589934592)
        self.assertEqual(next(sut), 8589934593)
        self.assertEqual(next(sut), 8589934594)
