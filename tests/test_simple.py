import pysparkling


def test_collect():
    my_rdd = pysparkling.Context().parallelize([1, 2, 3])
    assert my_rdd.collect()[0] == 1


def test_broadcast():
    b = pysparkling.Context().broadcast([1, 2, 3])
    assert b.value[0] == 1


def test_map():
    my_rdd = pysparkling.Context().parallelize([1, 2, 3]).map(lambda x: x+1)
    assert my_rdd.collect()[0] == 2


def test_foreach():
    my_rdd = pysparkling.Context().parallelize([1, 2, 3])
    my_rdd.foreach(lambda x: x+1)
    assert my_rdd.collect()[0] == 2


def test_countByValue():
    my_rdd = pysparkling.Context().parallelize([1, 2, 2, 4, 1])
    assert my_rdd.countByValue()[2] == 2


def test_countByKey():
    my_rdd = pysparkling.Context().parallelize([('a', 1), ('b', 2), ('b', 2)])
    assert my_rdd.countByKey()['b'] == 4


def test_flatMapValues():
    my_rdd = pysparkling.Context().parallelize([
        ('message', ('hello', 'world'))
    ])
    mapped = my_rdd.flatMapValues(lambda x: ['a']+list(x)).collect()
    assert mapped[0][1][0] == 'a'


def test_fold():
    my_rdd = pysparkling.Context().parallelize([4, 7, 2])
    folded = my_rdd.fold(0, lambda a, b: a+b)
    assert folded == 13


def test_foldByKey():
    my_rdd = pysparkling.Context().parallelize([('a', 4), ('b', 7), ('a', 2)])
    folded = my_rdd.foldByKey(0, lambda a, b: a+b)
    assert folded['a'] == 6


def test_groupBy():
    my_rdd = pysparkling.Context().parallelize([4, 7, 2])
    grouped = my_rdd.groupBy(lambda x: x % 2).collect()
    assert grouped[0][1][1] == 2


def test_take():
    my_rdd = pysparkling.Context().parallelize([4, 7, 2])
    assert my_rdd.take(2)[1] == 7


def test_takeSample():
    my_rdd = pysparkling.Context().parallelize([4, 7, 2])
    assert my_rdd.takeSample(1)[0] in [4, 7, 2]


def test_histogram():
    my_rdd = pysparkling.Context().parallelize([0, 4, 7, 4, 10])
    b, h = my_rdd.histogram(10)
    assert h[4] == 2


if __name__ == '__main__':
    test_collect()
    test_broadcast()
    test_map()
    test_foreach()
    test_countByValue()
    test_countByKey()
    test_flatMapValues()
    test_fold()
    test_foldByKey()
    test_take()
    test_takeSample()
    test_histogram()
