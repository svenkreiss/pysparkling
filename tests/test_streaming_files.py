from .streaming_test_case import StreamingTestCase


class TextFile(StreamingTestCase):

    def test_connect(self):
        self.result = 0
        self.expect = 78
        (
            self.stream_c.textFileStream('HISTORY.*')
            .count()
            .foreachRDD(lambda rdd: self.incr_result(rdd.collect()[0]))
        )

    def test_save(self):
        self.result = 0
        self.expect = 0
        (
            self.stream_c.textFileStream('HISTORY.*')
            .count()
            .saveAsTextFiles('tests/textout/')
        )

    def test_save_gz(self):
        self.result = 0
        self.expect = 0
        (
            self.stream_c.textFileStream('HISTORY.*')
            .count()
            .saveAsTextFiles('tests/textout/', suffix='.gz')
        )


class BinaryFile(StreamingTestCase):

    def test_read_file(self):
        self.result = 0
        self.expect = 1
        (
            self.stream_c.fileBinaryStream('HISTORY.*')
            .count()
            .foreachRDD(lambda rdd: self.incr_result(rdd.collect()[0]))
        )

    def test_read_chunks(self):
        self.result = 0
        self.expect = 100
        (
            self.stream_c.fileBinaryStream('HISTORY.*', recordLength=50)
            .count()
            .foreachRDD(lambda rdd: self.incr_result(rdd.collect()[0]))
        )
