from pysparkling import Context

my_rdd = Context().textFile('tests/*.py')

unfiltered_count = my_rdd.count()
filtered_count = my_rdd.filter(lambda l: l.startswith("import ")).count()
print(f'In tests/*.py: all lines={unfiltered_count}, with import={filtered_count}')
