from pysparkling import Context

my_rdd = Context().textFile('tests/*.py')
print('In tests/*.py: all lines={0}, with import={1}'.format(
  my_rdd.count(),
  my_rdd.filter(lambda l: l.startswith('import ')).count()
))
