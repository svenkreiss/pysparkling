import importlib
from importlib.abc import Loader, MetaPathFinder
import sys
import time
import warnings


class Pyspark2Pysparkling(MetaPathFinder, Loader):

    @classmethod
    def find_spec(cls, name, path, target=None):
        if name.startswith('pysparkling'):
            return None  # Nope. We're not providing this..

        if not name.startswith('pyspark'):
            return None  # Not handling these either...

        new_name = 'pysparkling' + name[7:]

        for finder in sys.meta_path[1:]:
            module_spec = finder.find_spec(fullname=new_name, path=path, target=target)
            if module_spec:
                break
        else:
            return None  # No finder could be found?

        module_spec.loader = cls(module_spec.loader)  # Inject my own loader here.

        return module_spec

    def __init__(self, old_loader: Loader):
        self.old_loader = old_loader

    def create_module(self, spec):
        # Delegate to the old loader
        return self.old_loader.create_module(spec)

    def exec_module(self, module):
        # Delegate to the old loader
        return_value = self.old_loader.exec_module(module)

        # Cache it for future use
        new_name = module.__name__
        old_name = 'pyspark' + new_name[11:]
        sys.modules[old_name] = module

        return return_value

    def module_repr(self, module):
        # Delegate to the old loader
        return self.old_loader.module_repr(module)

    @classmethod
    def is_enabled(cls):
        return cls in sys.meta_path

    @classmethod
    def setup(cls):
        sys.meta_path.insert(0, cls)

        # Find any already loaded 'pyspark' modules:
        modules_to_set = [
            (pyspark_name, 'pysparkling' + pyspark_name[7:])
            for pyspark_name in sys.modules
            if pyspark_name.startswith('pyspark') and not pyspark_name.startswith('pysparkling')
        ]

        if modules_to_set:
            warnings.warn(
                "pyspark was already loaded."
                " Please setup pysparkling first to ensure no nasty side-effects take place."
            )

        for pyspark_name, new_name in modules_to_set:
            try:
                # Pysparkling was already loaded?
                pysparkling_module = sys.modules[new_name]
            except KeyError:
                # Load it
                pysparkling_module = importlib.import_module(new_name)

            # And override it in the pyspark one
            sys.modules[pyspark_name] = pysparkling_module


def _test():
    # Comment or un-comment the next line to make the magic work...
    Pyspark2Pysparkling.setup()

    start = time.time()

    # pylint: disable=import-outside-toplevel
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local")
        .appName("SparkByExamples.com")
        .getOrCreate()
    )
    dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
    rdd = spark.sparkContext.parallelize(dataList)
    print(rdd.collect())

    data = [
        ('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1),
    ]

    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
    df = spark.createDataFrame(data=data, schema=columns)
    df.show()

    print(f"Finished this run in : {time.time() - start:.3f}s (Injector was on: {Pyspark2Pysparkling.is_enabled()})")


if __name__ == '__main__':
    _test()
