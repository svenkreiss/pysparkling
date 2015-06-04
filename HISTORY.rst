
Changelog
=========

* `master <https://github.com/svenkreiss/pysparkling/compare/v0.2.16...master>`_
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
