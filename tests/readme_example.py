import pysparkling

context = pysparkling.Context()
my_rdd = context.textFile('tests/*.py')
print('In tests/*.py: all lines={0}, with import={1}'.format(
    my_rdd.count(),
    my_rdd.filter(lambda l: l.startswith('import ')).count()
))
