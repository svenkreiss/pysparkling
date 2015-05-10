import pysparkling


def test_local_textFile_1():
    lines = pysparkling.Context().textFile('tests/*resolve*.py').collect()
    print(lines)
    assert 'import pysparkling' in lines


def test_local_textFile_2():
    line_count = pysparkling.Context().textFile('tests/*.py').count()
    print(line_count)
    assert line_count > 90


if __name__ == '__main__':
    test_local_textFile_1()
    test_local_textFile_2()
