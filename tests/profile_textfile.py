import tempfile

from memory_profiler import profile

import pysparkling


@profile
def main():
    tempFile = tempfile.NamedTemporaryFile(delete=True)
    tempFile.close()

    sc = pysparkling.Context()
    sc.parallelize(range(1000000)).saveAsTextFile(tempFile.name + '.gz')
    rdd = sc.textFile(tempFile.name + '.gz')
    rdd.collect()


if __name__ == '__main__':
    main()
