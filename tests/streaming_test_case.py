import pysparkling
import unittest


class StreamingTestCase(unittest.TestCase):

    def setUp(self):
        self.c = pysparkling.Context()
        self.stream_c = pysparkling.streaming.StreamingContext(self.c, 0.01)

    def tearDown(self):
        self.stream_c.start()
        self.stream_c.awaitTermination(timeout=0.1)

        self.assertEqual(self.result, self.expect)

    def incr_result(self, value):
        self.result += value

    def append_result(self, value):
        if value:
            self.result.append(value)
