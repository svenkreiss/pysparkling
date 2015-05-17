import pysparkling


def test_union():
    sc = pysparkling.Context()
    rdd1 = sc.parallelize(['Hello'])
    rdd2 = sc.parallelize(['World'])
    union = sc.union([rdd1, rdd2]).collect()
    assert len(union) == 2 and 'Hello' in union and 'World' in union


def test_broadcast():
    b = pysparkling.Context().broadcast([1, 2, 3])
    assert b.value[0] == 1


if __name__ == '__main__':
	test_union()
