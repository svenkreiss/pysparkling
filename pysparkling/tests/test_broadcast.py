import unittest

import pysparkling


class BroadcastTest(unittest.TestCase):
    def setUp(self) -> None:
        self.context = pysparkling.Context()

    def testSimple(self):
        b = self.context.broadcast([1, 2, 3, 4, 5])
        self.assertEqual(b.value, [1, 2, 3, 4, 5])

    def testAppendFails(self):
        b = self.context.broadcast([1, 2, 3, 4, 5])
        with self.assertRaises(AttributeError):
            b.value += [1]  # type: ignore
