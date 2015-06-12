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


def test_wholeTextFiles():
    t = Context().wholeTextFiles('tests/*.py').lookup('tests/test_textFile.py')
    print(t)
    assert 'test_wholeTextFiles' in t[0]


def test_s3_textFile():
    if not os.getenv('AWS_ACCESS_KEY_ID'):
        return

    myrdd = Context().textFile(
        's3n://aws-publicdatasets/common-crawl/crawl-data/'
        'CC-MAIN-2015-11/warc.paths.*'
    )
    assert (
        'common-crawl/crawl-data/CC-MAIN-2015-11/segments/1424937481488.49/'
        'warc/CC-MAIN-20150226075801-00329-ip-10-28-5-156.ec2.'
        'internal.warc.gz' in myrdd.collect()
    )


def test_http_textFile():
    myrdd = Context().textFile(
        'https://s3-us-west-2.amazonaws.com/human-microbiome-project/DEMO/'
        'HM16STR/46333/by_subject/1139.fsa'
    )
    assert u'TGCTGCGGTGAATGCGTTCCCGGGTCT' in myrdd.collect()


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


def test_pyspark_compatibility_txt():
    kv = Context().textFile('tests/pyspark/key_value.txt').collect()
    print(kv)
    assert u"('a', 1)" in kv and u"('b', 2)" in kv and len(kv) == 2


def test_pyspark_compatibility_bz2():
    kv = Context().textFile('tests/pyspark/key_value.txt.bz2').collect()
    print(kv)
    assert u"a\t1" in kv and u"b\t2" in kv and len(kv) == 2


def test_pyspark_compatibility_gz():
    kv = Context().textFile('tests/pyspark/key_value.txt.gz').collect()
    print(kv)
    assert u"a\t1" in kv and u"b\t2" in kv and len(kv) == 2


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    # test_saveAsTextFile()
    # test_local_textFile_2()
    test_wholeTextFiles()
    # test_saveAsTextFile_gz()
    # test_s3_textFile()
    # test_http_textFile()
    # test_pyspark_compatibility_txt()
    # test_pyspark_compatibility_gz()
    # test_pyspark_compatibility_bz2()
