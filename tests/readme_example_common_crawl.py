from pysparkling import Context

# read all the paths of warc and wat files of the latest Common Crawl
paths_rdd = Context().textFile(
    's3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2015-11/warc.paths.*,'
    's3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2015-11/wat.paths.gz'
)

print(paths_rdd.collect())
