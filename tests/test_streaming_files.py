from .streaming_test_case import StreamingTestCase


class TextFileTest(StreamingTestCase):

    def test_connect(self):
        self.result = 0
        self.expect = 78
        (
            self.stream_c.textFileStream('HISTORY.*')
            .count()
            .foreachRDD(lambda rdd: self.incr_result(rdd.collect()[0]))
        )
