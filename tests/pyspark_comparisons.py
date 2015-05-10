import pyspark

SC = pyspark.SparkContext()


def simple_textFile():
    print(SC.textFile('tests/test_simple.py').collect())


if __name__ == '__main__':
    simple_textFile()
