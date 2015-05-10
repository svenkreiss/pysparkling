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
    assert mapped[0][1][0] == 'a'


def test_fold():
    my_tld = pytld.Context().parallelize([4, 7, 2])
    folded = my_tld.fold(0, lambda a, b: a+b)
    assert folded == 13


def test_foldByKey():
    my_tld = pytld.Context().parallelize([('a', 4), ('b', 7), ('a', 2)])
    folded = my_tld.foldByKey(0, lambda a, b: a+b)
    assert folded['a'] == 6


def test_groupBy():
    my_tld = pytld.Context().parallelize([4, 7, 2])
    grouped = my_tld.groupBy(lambda x: x % 2).collect()
    assert grouped[0][1][1] == 2


if __name__ == '__main__':
    test_collect()
    test_map()
    test_foreach()
    test_countByValue()
    test_countByKey()
    test_flatMapValues()
    test_fold()
    test_foldByKey()
