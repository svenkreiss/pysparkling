import pysparkling


def main():
    sc = pysparkling.Context()
    ssc = pysparkling.streaming.StreamingContext(sc, 1)
    ssc.textFileStream('/var/log/system.log*').pprint()
    ssc.start()
    ssc.awaitTermination(timeout=3.0)


if __name__ == '__main__':
    main()
