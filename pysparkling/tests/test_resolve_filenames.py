from __future__ import print_function

import os

import pytest

from pysparkling.fileio import File

CURRENT_FILE_LOCATION = __file__


class MockedHdfsClient(object):
    def list(self, path, status):
        if path == "/user/username/":
            return [
                ("input", {"type": "DIRECTORY"}),
                ("output", {"type": "DIRECTORY"})
            ]
        if path in ('/user/username/input', '/user/username/input/'):
            return [
                ("part-00001.gz", {"type": "FILE"}),
                ("part-00002.gz", {"type": "FILE"}),
                ("_SUCCESS", {"type": "FILE"})
            ]
        raise NotImplementedError(f"Return value not mocked for '{path}'")


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
        f'{os.path.dirname(CURRENT_FILE_LOCATION)}/*'
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


def test_hdfs_resolve_filenames_with_wildcard():
    # hdfs is an optional dependency
    # pylint: disable=import-outside-toplevel
    from pysparkling.fileio.fs import Hdfs
    Hdfs.client_and_path = staticmethod(lambda *args, **kwargs: (MockedHdfsClient(), "unused_path"))

    filenames = Hdfs.resolve_filenames("hdfs://hdfs-cluster.com/user/username/input/part-*.gz")
    print(filenames)
    assert filenames == [
        'hdfs://hdfs-cluster.com/user/username/input/part-00001.gz',
        'hdfs://hdfs-cluster.com/user/username/input/part-00002.gz'
    ]


def test_hdfs_resolve_filenames_with_folder_path():
    # hdfs is an optional dependency
    # pylint: disable=import-outside-toplevel
    from pysparkling.fileio.fs import Hdfs
    Hdfs.client_and_path = staticmethod(lambda *args, **kwargs: (MockedHdfsClient(), "unused_path"))

    filenames = Hdfs.resolve_filenames("hdfs://hdfs-cluster.com/user/username/input")
    print(filenames)
    assert filenames == [
        'hdfs://hdfs-cluster.com/user/username/input/part-00001.gz',
        'hdfs://hdfs-cluster.com/user/username/input/part-00002.gz'
    ]


def test_hdfs_resolve_filenames_with_folder_path_and_trailing_slash():
    # hdfs is an optional dependency
    # pylint: disable=import-outside-toplevel
    from pysparkling.fileio.fs import Hdfs
    Hdfs.client_and_path = staticmethod(lambda *args, **kwargs: (MockedHdfsClient(), "unused_path"))

    filenames = Hdfs.resolve_filenames("hdfs://hdfs-cluster.com/user/username/input/")
    print(filenames)
    assert filenames == [
        'hdfs://hdfs-cluster.com/user/username/input/part-00001.gz',
        'hdfs://hdfs-cluster.com/user/username/input/part-00002.gz'
    ]


def test_hdfs_resolve_filenames_with_file_path():
    # hdfs is an optional dependency
    # pylint: disable=import-outside-toplevel
    from pysparkling.fileio.fs import Hdfs
    Hdfs.client_and_path = staticmethod(lambda *args, **kwargs: (MockedHdfsClient(), "unused_path"))

    filenames = Hdfs.resolve_filenames("hdfs://hdfs-cluster.com/user/username/input/part-00001.gz")
    print(filenames)
    assert filenames == [
        'hdfs://hdfs-cluster.com/user/username/input/part-00001.gz'
    ]


def test_s3_resolve_filenames():
    # boto is an optional dependency
    # pylint: disable=import-outside-toplevel
    from pysparkling.fileio.fs import S3
    S3._get_conn = classmethod(lambda *args, **kwargs: MockedS3Connection())

    filenames = S3.resolve_filenames("s3://bucket-name/user/username/input/part-*.gz")
    print(filenames)
    assert filenames == [
        's3://bucket-name/user/username/input/part-00001.gz',
        's3://bucket-name/user/username/input/part-00002.gz'
    ]


if __name__ == '__main__':
    test_local_1()
    test_local_2()
    test_s3_1()
    test_hdfs_resolve_filenames_with_folder_path()
    test_hdfs_resolve_filenames_with_folder_path_and_trailing_slash()
    test_hdfs_resolve_filenames_with_file_path()
    test_hdfs_resolve_filenames_with_wildcard()
    test_s3_resolve_filenames()
