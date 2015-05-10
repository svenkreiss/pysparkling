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


def test_countByValue():
    my_tld = pytld.Context().parallelize([1, 2, 2, 4, 1])
    assert my_tld.countByValue()[2] == 2


def test_countByKey():
    my_tld = pytld.Context().parallelize([('a', 1), ('b', 2), ('b', 2)])
    assert my_tld.countByKey()['b'] == 4


def test_flatMapValues():
    my_tld = pytld.Context().parallelize([('message', ('hello', 'world'))])
    mapped = my_tld.flatMapValues(lambda x: ['a']+list(x)).collect()
    print(mapped)
    assert mapped[0][1][0] == 'a'


if __name__ == '__main__':
    test_collect()
    test_map()
    test_foreach()
    test_countByValue()
    test_countByKey()
    test_flatMapValues()
