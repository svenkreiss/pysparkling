import datetime
import os
import shutil
from unittest import TestCase

from dateutil.tz import tzlocal

from pysparkling import Context, Row
from pysparkling.sql.session import SparkSession
from pysparkling.sql.utils import AnalysisException

spark = SparkSession(Context())


def get_folder_content(folder_path):
    folder_content = {}
    for root, _, files in os.walk(folder_path):
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
            [Row(age=2, name='Alice', time=datetime.datetime(2017, 1, 1, tzinfo=tzlocal()), ),
             Row(age=5, name='Bob', time=datetime.datetime(2014, 3, 2, tzinfo=tzlocal()))]
        )
        df.write.csv(".tmp/wonderland/")
        self.assertDictEqual(
            get_folder_content(".tmp/wonderland"),
            {
                '_SUCCESS': [],
                'part-00000-65653853369276080.csv': [
                    '2,Alice,2017-01-01T00:00:00.000+01:00\n',
                    '5,Bob,2014-03-02T00:00:00.000+01:00\n'
                ]
            }
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
             'part-00000-4061950540148431296.csv': [
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
             'part-00000-3434325560268771971.csv': [
                 '2,Alice\n',
                 '5,Bob\n'
             ]}
        )

    def test_write_to_json(self):
        df = spark.createDataFrame(
            [Row(age=2, name='Alice', time=datetime.datetime(2017, 1, 1, tzinfo=tzlocal()), ),
             Row(age=5, name='Bob', time=datetime.datetime(2014, 3, 2, tzinfo=tzlocal()))]
        )
        df.write.json(".tmp/wonderland/")
        self.assertDictEqual(
            get_folder_content(".tmp/wonderland"),

            {'_SUCCESS': [],
             'part-00000-65653853369276080.json': [
                 '{"age":2,"name":"Alice","time":"2017-01-01T00:00:00.000+01:00"}\n',
                 '{"age":5,"name":"Bob","time":"2014-03-02T00:00:00.000+01:00"}\n'
             ]}
        )

    def test_write_nested_rows_to_json(self):
        df = spark.createDataFrame(
            [Row(age=2, name='Alice', animals=[
                Row(name="Chessur", type="cat"),
                Row(name="The White Rabbit", type="Rabbit")
            ]),
             Row(age=5, name='Bob', animals=[])]
        )
        df.write.json(".tmp/wonderland/")
        self.assertDictEqual(
            get_folder_content(".tmp/wonderland"),
            {'_SUCCESS': [],
             'part-00000-2819354714706678872.json': [
                 '{"age":2,"animals":['
                 '{"name":"Chessur","type":"cat"},'
                 '{"name":"The White Rabbit","type":"Rabbit"}'
                 '],"name":"Alice"}\n',
                 '{"age":5,"animals":[],"name":"Bob"}\n'
             ]}
        )
