import pyspark

SC = pyspark.SparkContext()


def simple_textFile():
    print(SC.textFile('tests/test_simple.py').collect())
    print(SC.textFile('tests/test_simple.py').name())
    print(SC.parallelize([1, 2, 3]).name())


def indent_line(l):
    print('============== INDENTING LINE ================')
    return '--- ' + l


def lazy_execution():
    r = SC.textFile('tests/test_simple.py').map(indent_line)
    r.foreach(indent_line)
    print()
    print()
    print()
    # at this point, no map() or foreach() should have been executed
    print(r.collect())


def count_lines():
    r = SC.wholeTextFiles('tests/*.py').keys().collect()
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    print(r)
    print(SC.textFile('tests/*.py').count())


def create_key_value_txt():
    r = SC.parallelize([('a', 1), ('b', 2)], 1)
    r.saveAsTextFile('tests/pyspark/key_value.txt')
    r.saveAsHadoopFile(
        "tests/pyspark/key_value.txt.bz2",
        "org.apache.hadoop.mapred.TextOutputFormat",
        compressionCodecClass="org.apache.hadoop.io.compress.BZip2Codec",
    )
    r.saveAsHadoopFile(
        "tests/pyspark/key_value.txt.gz",
        "org.apache.hadoop.mapred.TextOutputFormat",
        compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
    )
    # r.saveAsHadoopFile(
    #     "tests/pyspark/key_value.txt.lzo",
    #     "org.apache.hadoop.mapred.TextOutputFormat",
    #     compressionCodecClass="com.hadoop.compression.lzo.LzopCodec",
    # )

    r_txt = SC.textFile('tests/pyspark/key_value.txt')
    print(r_txt.collect())
    r_gz = SC.textFile('tests/pyspark/key_value.txt.gz')
    print(r_gz.collect())
    r_bz2 = SC.textFile('tests/pyspark/key_value.txt.bz2')
    print(r_bz2.collect())


def create_pickled_files():
    rdd = SC.parallelize(['hello', 'world', 1, 2], 2)
    rdd.saveAsPickleFile('tests/pyspark/mixed.pickle')
    rdd.saveAsPickleFile('tests/pyspark/mixed_batched.pickle', 1)


def stat():
    d = [1, 4, 9, 16, 25, 36]
    s1 = SC.parallelize(d).stats()
    s2 = SC.parallelize(d, 3).stats()
    print(str(s1))
    print(str(s2))


def partition_by():
    rdd = SC.parallelize(range(20), 2).map(lambda x: (x, x))
    r = rdd.partitionBy(2).collect()
    print('>>>>>>', r)


if __name__ == '__main__':
    # simple_textFile()
    # lazy_execution()
    # count_lines()
    # create_key_value_txt()
    # create_pickled_files()
    # stat()
    partition_by()
