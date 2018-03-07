from __future__ import print_function

import os
from pysparkling.fileio import File
import pytest


CURRENT_FILE_LOCATION = __file__


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


if __name__ == '__main__':
    test_local_1()
    test_local_2()
    test_s3_1()
