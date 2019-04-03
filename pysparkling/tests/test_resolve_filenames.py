from __future__ import print_function

import os

import pytest

from pysparkling.fileio import File

CURRENT_FILE_LOCATION = __file__


class MockedHdfsClient(object):
    def list(self, *args, **kwargs):
        return ["part-00001.gz", "part-00002.gz", "_SUCCESS"]


class MockedS3Bucket(object):
    def list(self, *args, **kwargs):
        return [
            MockedS3Key("user/username/input/part-00001.gz"),
            MockedS3Key("user/username/input/part-00002.gz"),
            MockedS3Key("user/username/input/_SUCCESS"),
        ]


class MockedS3Connection(object):
    def get_bucket(self, *args, **kwargs):
        return MockedS3Bucket()


class MockedS3Key(object):
    def __init__(self, name):
        self.name = name


def test_local_1():
    filenames = File.resolve_filenames(
        '{}/*'.format(os.path.dirname(CURRENT_FILE_LOCATION))
    )
    assert CURRENT_FILE_LOCATION in filenames


def test_local_2():
    filenames = File.resolve_filenames(CURRENT_FILE_LOCATION)
    assert filenames == [CURRENT_FILE_LOCATION]


@pytest.mark.skipif(not os.getenv('AWS_ACCESS_KEY_ID'), reason='no AWS env')
def test_s3_1():
    filenames = File.resolve_filenames(
        's3n://aws-publicdatasets/common-crawl/'
        'crawl-data/CC-MAIN-2015-11/warc.paths.*'
    )
    print(filenames)
    assert ('s3n://aws-publicdatasets/common-crawl/'
            'crawl-data/CC-MAIN-2015-11/warc.paths.gz' in filenames)


def test_hdfs_resolve_filenames():
    from pysparkling.fileio.fs import Hdfs
    unused_path = "/"
    path = "hdfs://hdfs-cluster.com/user/username/input/part-*.gz"
    Hdfs.client_and_path = staticmethod(lambda *args, **kwargs: (MockedHdfsClient(), unused_path))

    filenames = Hdfs.resolve_filenames(path)
    print(filenames)
    assert filenames == [
        'hdfs://hdfs-cluster.com/user/username/input/part-00001.gz',
        'hdfs://hdfs-cluster.com/user/username/input/part-00002.gz'
    ]


def test_s3_resolve_filenames():
    from pysparkling.fileio.fs import S3
    S3._get_conn = classmethod(lambda *args, **kwargs: MockedS3Connection())
    path = "s3://bucket-name/user/username/input/part-*.gz"
    filenames = S3.resolve_filenames(path)

    print(filenames)
    assert filenames == [
        's3://bucket-name/user/username/input/part-00001.gz',
        's3://bucket-name/user/username/input/part-00002.gz'
    ]


if __name__ == '__main__':
    test_local_1()
    test_local_2()
    test_s3_1()
    test_hdfs_resolve_filenames()
    test_s3_resolve_filenames()
