import pytld


def test_collect():
    my_tld = pytld.Context().parallelize([1, 2, 3])
    assert my_tld.collect()[0] == 1


def test_map():
    my_tld = pytld.Context().parallelize([1, 2, 3]).map(lambda x: x+1)
    assert my_tld.collect()[0] == 2


def test_foreach():
    my_tld = pytld.Context().parallelize([1, 2, 3])
    my_tld.foreach(lambda x: x+1)
    assert my_tld.collect()[0] == 2


if __name__ == '__main__':
    test_collect()
    test_map()
    test_foreach()
