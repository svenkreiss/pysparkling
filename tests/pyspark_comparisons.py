import pyspark

SC = pyspark.SparkContext()


def simple_textFile():
    print(SC.textFile('tests/test_simple.py').collect())
    print(SC.textFile('tests/test_simple.py').name())
    print(SC.parallelize([1, 2, 3]).name())


def indent_line(l):
    print('============== INDENTING LINE ================')
    return '--- '+l


def lazy_execution():
    r = SC.textFile('tests/test_simple.py').map(indent_line)
    r.foreach(indent_line)
    print()
    print()
    print()
    # at this point, no map() or foreach() should have been executed
    print(r.collect())


if __name__ == '__main__':
    simple_textFile()
    # lazy_execution()
