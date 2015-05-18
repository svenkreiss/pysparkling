from __future__ import print_function

import logging
import tempfile
from pysparkling import Context


def test_aggregate():
    seqOp = (lambda x, y: (x[0] + y, x[1] + 1))
    combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
    r = Context().parallelize([1, 2, 3, 4]).aggregate((0, 0), seqOp, combOp)
    assert r[0] == 10 and r[1] == 4


def test_aggregateByKey():
    seqOp = (lambda x, y: x + y)
    combOp = (lambda x, y: x + y)
    r = Context().parallelize([('a', 1), ('b', 2), ('a', 3), ('c', 4)]).aggregateByKey(int, seqOp, combOp)
    assert r['a'] == 4 and r['b'] == 2

def test_cartesian():
    rdd = Context().parallelize([1, 2])
    r = sorted(rdd.cartesian(rdd).collect())
    print(r)
    assert r[0][0] == 1 and r[2][0] == 2 and len(r) == 4 and len(r[0]) == 2

def test_coalesce():
    my_rdd = Context().parallelize([1, 2, 3], 2).coalesce(1)
    assert my_rdd.getNumPartitions() == 1


def test_collect():
    my_rdd = Context().parallelize([1, 2, 3])
    assert my_rdd.collect()[0] == 1


def test_count():
    my_rdd = Context().parallelize([1, 2, 3])
    assert my_rdd.count() == 3


def test_count_partitions():
    my_rdd = Context().parallelize([1, 2, 3], 2)
    print(my_rdd.collect())
    my_rdd.foreach(print)
    assert my_rdd.count() == 3


def test_countByKey():
    my_rdd = Context().parallelize([('a', 1), ('b', 2), ('b', 2)])
    assert my_rdd.countByKey()['b'] == 4


def test_countByValue():
    my_rdd = Context().parallelize([1, 2, 2, 4, 1])
    assert my_rdd.countByValue()[2] == 2


def test_distinct():
    my_rdd = Context().parallelize([1, 2, 2, 4, 1]).distinct()
    assert my_rdd.count() == 3


def test_filter():
    my_rdd = Context().parallelize([1, 2, 2, 4, 1, 3, 5, 9], 3).filter(lambda x: x % 2 == 0)
    print(my_rdd.collect())
    print(my_rdd.count())
    assert my_rdd.count() == 3


def test_first():
    my_rdd = Context().parallelize([1, 2, 2, 4, 1, 3, 5, 9], 3)
    print(my_rdd.first())
    assert my_rdd.first() == 1    


def test_flatMap():
    my_rdd = Context().parallelize([
        ('hello', 'world')
    ])
    mapped = my_rdd.flatMap(lambda x: [['a']+list(x)]).collect()
    assert mapped[0][0] == 'a'


def test_flatMapValues():
    my_rdd = Context().parallelize([
        ('message', ('hello', 'world'))
    ])
    mapped = my_rdd.flatMapValues(lambda x: ['a']+list(x)).collect()
    assert mapped[0][1][0] == 'a'


def test_fold():
    my_rdd = Context().parallelize([4, 7, 2])
    folded = my_rdd.fold(0, lambda a, b: a+b)
    assert folded == 13


def test_foldByKey():
    my_rdd = Context().parallelize([('a', 4), ('b', 7), ('a', 2)])
    folded = my_rdd.foldByKey(0, lambda a, b: a+b)
    assert folded['a'] == 6


def test_foreach():
    my_rdd = Context().parallelize([1, 2, 3])
    my_rdd.foreach(lambda x: x+1)
    assert my_rdd.collect()[0] == 1


def test_groupBy():
    my_rdd = Context().parallelize([4, 7, 2])
    grouped = my_rdd.groupBy(lambda x: x % 2).collect()
    print(grouped)
    assert grouped[0][1][0] == 2


def test_histogram():
    my_rdd = Context().parallelize([0, 4, 7, 4, 10])
    b, h = my_rdd.histogram(10)
    assert h[4] == 2


def test_intersection():
    rdd1 = Context().parallelize([0, 4, 7, 4, 10])
    rdd2 = Context().parallelize([3, 4, 7, 4, 5])
    i = rdd1.intersection(rdd2)
    assert i.collect()[0] == 4


def test_join():
    rdd1 = Context().parallelize([(0, 1), (1, 1)])
    rdd2 = Context().parallelize([(2, 1), (1, 3)])
    j = rdd1.join(rdd2)
    assert dict(j.collect())[1][1] == 3


def test_keyBy():
    rdd = Context().parallelize([0, 4, 7, 4, 10])
    rdd = rdd.keyBy(lambda x: x % 2)
    assert rdd.collect()[2][0] == 1  # the third element (7) is odd


def test_keys():
    rdd = Context().parallelize([(0, 1), (1, 1)]).keys()
    assert rdd.collect()[0] == 0


def test_leftOuterJoin():
    rdd1 = Context().parallelize([(0, 1), (1, 1)])
    rdd2 = Context().parallelize([(2, 1), (1, 3)])
    j = rdd1.leftOuterJoin(rdd2)
    assert dict(j.collect())[1][1] == 3


def test_lookup():
    rdd = Context().parallelize([(0, 1), (1, 1), (1, 3)])
    print(rdd.lookup(1))
    assert 3 in rdd.lookup(1)


def test_map():
    my_rdd = Context().parallelize([1, 2, 3]).map(lambda x: x+1)
    assert my_rdd.collect()[0] == 2


def test_mapPartitions():
    rdd = Context().parallelize([1, 2, 3, 4], 2)
    def f(iterator): yield sum(iterator)
    r = rdd.mapPartitions(f).collect()
    assert 3 in r and 7 in r


def test_max():
    rdd = Context().parallelize([1, 2, 3, 4, 3, 2], 2)
    assert rdd.max() == 4


def test_mean():
    rdd = Context().parallelize([0, 4, 7, 4, 10])
    assert rdd.mean() == 5


def test_pipe():
    rdd = Context().parallelize(['0', 'hello', 'world'])
    piped = rdd.pipe('echo').collect()
    print(piped)
    assert b'hello\n' in piped


def test_reduce():
    rdd = Context().parallelize([0, 4, 7, 4, 10])
    assert rdd.reduce(lambda a, b: a+b) == 25


def test_reduceByKey():
    rdd = Context().parallelize([(0, 1), (1, 1), (1, 3)])
    assert dict(rdd.reduceByKey(lambda a, b: a+b).collect())[1] == 4


def test_rightOuterJoin():
    rdd1 = Context().parallelize([(0, 1), (1, 1)])
    rdd2 = Context().parallelize([(2, 1), (1, 3)])
    j = rdd1.rightOuterJoin(rdd2)
    assert dict(j.collect())[1][1] == 3


def test_saveAsTextFile():
    tempFile = tempfile.NamedTemporaryFile(delete=True)
    tempFile.close()
    Context().parallelize(range(10)).saveAsTextFile(tempFile.name)
    with open(tempFile.name+'/part-00000', 'r') as f:
        r = f.readlines()
        print(r)
        assert '5\n' in r


def test_subtract():
    rdd1 = Context().parallelize([(0, 1), (1, 1)])
    rdd2 = Context().parallelize([(1, 1), (1, 3)])
    subtracted = rdd1.subtract(rdd2).collect()
    assert (0, 1) in subtracted and (1, 1) not in subtracted


def test_sum():
    rdd = Context().parallelize([0, 4, 7, 4, 10])
    assert rdd.sum() == 25


def test_take():
    my_rdd = Context().parallelize([4, 7, 2])
    assert my_rdd.take(2)[1] == 7


def test_takeSample():
    my_rdd = Context().parallelize([4, 7, 2])
    assert my_rdd.takeSample(1)[0] in [4, 7, 2]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    test_filter()
