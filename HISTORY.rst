
Changelog
=========

* `master <https://github.com/svenkreiss/pysparkling/compare/v0.3.19...master>`_
* `v0.3.19 <https://github.com/svenkreiss/pysparkling/compare/v0.3.18...v0.3.19>`_ (2016-03-06)
    * removed use of ``itertools.tee()`` and replaced with clear ownership of partitions and partition data
    * replace some remaining use of ``str()`` with ``format()``
    * bugfix for ``RDD.groupByKey()`` and ``RDD.reduceByKey()`` for non-hashable values by @pganssle
    * small updates to docs and their build process
* `v0.3.18 <https://github.com/svenkreiss/pysparkling/compare/v0.2.28...v0.3.18>`_ (2016-02-13)
    * bring docs and Github releases back in sync
    * ... many updates.
* `v0.2.28 <https://github.com/svenkreiss/pysparkling/compare/v0.2.24...v0.2.28>`_ (2015-07-03)
    * implement ``RDD.sortBy()`` and ``RDD.sortByKey()``
    * additional unit tests
* `v0.2.24 <https://github.com/svenkreiss/pysparkling/compare/v0.2.23...v0.2.24>`_ (2015-06-16)
    * replace dill with cloudpickle in docs and test
    * add tests with pypy and pypy3
* `v0.2.23 <https://github.com/svenkreiss/pysparkling/compare/v0.2.22...v0.2.23>`_ (2015-06-15)
    * added RDD.randomSplit()
    * saveAsTextFile() saves single file if there is only one partition (and does not break it out into partitions)
* `v0.2.22 <https://github.com/svenkreiss/pysparkling/compare/v0.2.21...v0.2.22>`_ (2015-06-12)
    * added Context.wholeTextFiles()
    * improved RDD.first() and RDD.take(n)
    * added fileio.TextFile
* `v0.2.21 <https://github.com/svenkreiss/pysparkling/compare/v0.2.19...v0.2.21>`_ (2015-06-07)
    * added doc strings and created Sphinx documentation
    * implemented allowLocal in ``Context.runJob()``
* `v0.2.19 <https://github.com/svenkreiss/pysparkling/compare/v0.2.16...v0.2.19>`_ (2015-06-04)
    * new IPython demo notebook at ``docs/demo.ipynb`` at https://github.com/svenkreiss/pysparkling/blob/master/docs/demo.ipynb
    * ``parallelize()`` can take an iterator (used in ``zip()`` now for lazy loading)
* `v0.2.16 <https://github.com/svenkreiss/pysparkling/compare/v0.2.13...v0.2.16>`_ (2015-05-31)
    * add ``values()``, ``union()``, ``zip()``, ``zipWithUniqueId()``, ``toLocalIterator()``
    * improve ``aggregate()`` and ``fold()``
    * add ``stats()``, ``sampleStdev()``, ``sampleVariance()``, ``stdev()``, ``variance()``
    * make ``cache()`` and ``persist()`` do something useful
    * better partitioning in ``parallelize()``
    * logo
    * fix ``foreach()``
* `v0.2.10 <https://github.com/svenkreiss/pysparkling/compare/v0.2.8...v0.2.10>`_ (2015-05-27)
    * fix ``fileio.codec`` import
    * support ``http://``
* `v0.2.8 <https://github.com/svenkreiss/pysparkling/compare/v0.2.6...v0.2.8>`_ (2015-05-26)
    * parallelized text file reading (and made it lazy)
    * parallelized take() and takeSample() that only computes required data partitions
    * add example: access Human Microbiome Project
* `v0.2.6 <https://github.com/svenkreiss/pysparkling/compare/v0.2.2...v0.2.6>`_ (2015-05-21)
    * factor out ``fileio.fs`` and ``fileio.codec`` modules
    * merge ``WholeFile`` into ``File``
    * improved handling of compressed files (backwards incompatible)
    * ``fileio`` interface changed to ``dump()`` and ``load()`` methods. Added ``make_public()`` for S3.
    * factor file related operations into ``fileio`` submodule
* `v0.2.2 <https://github.com/svenkreiss/pysparkling/compare/v0.2.0...v0.2.2>`_ (2015-05-18)
    * compressions: ``.gz``, ``.bz2``
* `v0.2.0 <https://github.com/svenkreiss/pysparkling/compare/v0.1.1...v0.2.0>`_ (2015-05-17)
    * proper handling of partitions
    * custom serializers, deserializers (for functions and data separately)
    * more tests for parallelization options
    * execution of distributed jobs is such that a chain of ``map()`` operations gets executed on workers without sending intermediate results back to the master
    * a few more methods for RDDs implemented
* `v0.1.1 <https://github.com/svenkreiss/pysparkling/compare/v0.1.0...v0.1.1>`_ (2015-05-12)
    * implemented a few more RDD methods
    * changed handling of context in RDD
* v0.1.0 (2015-05-09)
