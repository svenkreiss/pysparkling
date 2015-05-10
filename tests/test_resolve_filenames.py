import pysparkling


def test_local_1():
    filenames = pysparkling.Context()._resolve_filenames(
        'tests/*'
    )
    assert 'tests/test_simple.py' in filenames


def test_local_2():
    filenames = pysparkling.Context()._resolve_filenames(
        'tests/test_simple.py'
    )
    assert 'tests/test_simple.py' == filenames[0] and len(filenames) == 1


if __name__ == '__main__':
    test_local_1()
    test_local_2()
