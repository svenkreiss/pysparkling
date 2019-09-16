import datetime
import os
import shutil
import time as _time
from unittest import TestCase

from pysparkling import Context, Row
from pysparkling.sql.session import SparkSession
from pysparkling.sql.utils import AnalysisException

spark = SparkSession(Context())
tz_local = datetime.timezone(datetime.timedelta(seconds=-(_time.altzone if _time.daylight else _time.timezone)))


def get_folder_content(folder_path):
    folder_content = {}
    for root, dirs, files in os.walk(folder_path):
        relative_path = root[len(folder_path):]
        for file in files:
            file_path = os.path.join(root, file)
            with open(file_path, 'r') as file_content:
                folder_content[os.path.join(relative_path, file)] = file_content.readlines()
    return folder_content


class DataFrameWriterTests(TestCase):
    maxDiff = None

    @staticmethod
    def clean():
        if os.path.exists(".tmp"):
            shutil.rmtree(".tmp")

    def setUp(self):
        self.clean()

    def tearDown(self):
        self.clean()

    def test_write_to_csv(self):
        df = spark.createDataFrame(
            [Row(age=2, name='Alice', time=datetime.datetime(2017, 1, 1, tzinfo=tz_local), ),
             Row(age=5, name='Bob', time=datetime.datetime(2014, 3, 2, tzinfo=tz_local))]
        )
        df.write.csv(".tmp/wonderland/")
        self.assertDictEqual(
            get_folder_content(".tmp/wonderland"),
            {'_SUCCESS': [],
             'part-00000-384289621793791622.csv': [
                 '2,Alice,2016-12-31T23:00:00.000+01:00\n',
                 '5,Bob,2014-03-01T23:00:00.000+01:00\n'
             ]}
        )

    def test_write_to_csv_with_custom_options(self):
        df = spark.createDataFrame(
            [
                Row(age=2, name='Alice', occupation=None),
                Row(age=5, name='Bob', occupation=""),
            ]
        )
        df.write.csv(".tmp/wonderland/", sep="^", emptyValue="", nullValue="null", header=True)
        self.assertDictEqual(
            get_folder_content(".tmp/wonderland"),
            {'_SUCCESS': [],
             'part-00000-384289621784791593.csv': [
                 'age^name^occupation\n',
                 '2^Alice^null\n',
                 '5^Bob^\n'
             ]}
        )

    def test_write_to_csv_fail_when_overwrite(self):
        df = spark.createDataFrame(
            [Row(age=2, name='Alice'),
             Row(age=5, name='Bob')]
        )
        df.write.csv(".tmp/wonderland/")
        with self.assertRaises(AnalysisException) as ctx:
           df.write.csv(".tmp/wonderland/")
        self.assertEqual(ctx.exception.args[0], 'path .tmp/wonderland already exists.;')
        self.assertDictEqual(
            get_folder_content(".tmp/wonderland"),
            {'_SUCCESS': [],
             'part-00000-3432532549516755848.csv': [
                 '2,Alice\n',
                 '5,Bob\n'
             ]}
        )
