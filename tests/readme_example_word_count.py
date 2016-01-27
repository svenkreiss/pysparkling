from __future__ import print_function

from pysparkling import Context

counts = Context().textFile(
    'README.rst'
).flatMap(
    lambda line: line.split(' ')
).map(
    lambda word: (word, 1)
).reduceByKey(
    lambda a, b: a + b
)
print(counts.collect())
