import os
import logging
import tempfile
from pysparkling import Context


def test_local_textFile_1():
    lines = Context().textFile('tests/*textFil*.py').collect()
    print(lines)
    assert 'from pysparkling import Context' in lines


def test_local_textFile_2():
    line_count = Context().textFile('tests/*.py').count()
    print(line_count)
    assert line_count > 90


def test_local_textFile_name():
    name = Context().textFile('tests/*.py').name()
    print(name)
    assert name == 'tests/*.py'


def test_s3_textFile():
    if not os.getenv('AWS_ACCESS_KEY_ID'):
        return

    myrdd = Context().textFile(
        's3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2015-11/warc.paths.*'
    )
    print(myrdd.collect())


def test_saveAsTextFile():
    tempFile = tempfile.NamedTemporaryFile(delete=True)
    tempFile.close()
    Context().parallelize(range(10)).saveAsTextFile(tempFile.name)
    with open(tempFile.name+'/part-00000', 'r') as f:
        r = f.readlines()
        print(r)
        assert '5\n' in r


def test_saveAsTextFile_gz():
    tempFile = tempfile.NamedTemporaryFile(delete=True)
    tempFile.close()
    Context().parallelize(range(10)).saveAsTextFile(tempFile.name+'.gz')
    read_rdd = Context().textFile(tempFile.name+'.gz')
    assert '5' in read_rdd.collect()


def test_saveAsTextFile_bz2():
    tempFile = tempfile.NamedTemporaryFile(delete=True)
    tempFile.close()
    Context().parallelize(range(10)).saveAsTextFile(tempFile.name+'.bz2')
    read_rdd = Context().textFile(tempFile.name+'.bz2')
    assert '5' in read_rdd.collect()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    test_local_textFile_2()
    # test_saveAsTextFile_gz()
