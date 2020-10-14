import datetime
import os
from unittest import TestCase

from pysparkling import Context, Row
from pysparkling.sql.session import SparkSession
from pysparkling.sql.types import StructType, StructField, TimestampType, StringType, \
    IntegerType, DateType

spark = SparkSession(Context())


class DataFrameReaderTests(TestCase):
    maxDiff = None

    def test_csv_read_without_schema(self):
        df = spark.read.csv(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "data/fundings/"
            ),
            header=True
        )
        self.assertEqual(
            df.count(),
            4
        )
        self.assertEqual(
            df.schema,
            StructType([
                StructField("permalink", StringType()),
                StructField("company", StringType()),
                StructField("numEmps", StringType()),
                StructField("category", StringType()),
                StructField("city", StringType()),
                StructField("state", StringType()),
                StructField("fundedDate", StringType()),
                StructField("raisedAmt", StringType()),
                StructField("raisedCurrency", StringType()),
                StructField("round", StringType())
            ])
        )
        self.assertListEqual(
            [Row(**r.asDict()) for r in df.collect()],
            [Row(permalink='mycityfaces', company='MyCityFaces', numEmps='7', category='web',
                 city='Scottsdale', state='AZ', fundedDate='2008-01-01', raisedAmt='50000',
                 raisedCurrency='USD', round='seed'),
             Row(permalink='flypaper', company='Flypaper', numEmps=None, category='web',
                 city='Phoenix', state='AZ', fundedDate='2008-02-01', raisedAmt='3000000',
                 raisedCurrency='USD', round='a'),
             Row(permalink='chosenlist-com', company='ChosenList.com', numEmps='5', category='web',
                 city='Scottsdale', state='AZ', fundedDate='2008-01-25', raisedAmt='233750',
                 raisedCurrency='USD', round='angel'),
             Row(permalink='digg', company='Digg', numEmps='60', category='web',
                 city='San Francisco', state='CA', fundedDate='2006-12-01', raisedAmt='8500000',
                 raisedCurrency='USD', round='b')]
        )

    def test_csv_read_with_inferred_schema(self):
        df = spark.read.option("inferSchema", True).csv(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "data/fundings/"
            ),
            header=True
        )
        self.assertEqual(
            df.count(),
            4
        )
        self.assertEqual(
            df.schema,
            StructType([
                StructField("permalink", StringType()),
                StructField("company", StringType()),
                StructField("numEmps", IntegerType()),
                StructField("category", StringType()),
                StructField("city", StringType()),
                StructField("state", StringType()),
                StructField("fundedDate", TimestampType()),
                StructField("raisedAmt", IntegerType()),
                StructField("raisedCurrency", StringType()),
                StructField("round", StringType())
            ])
        )
        self.assertEqual(
            [Row(**r.asDict()) for r in df.collect()],
            [Row(permalink='mycityfaces', company='MyCityFaces', numEmps=7, category='web',
                 city='Scottsdale', state='AZ', fundedDate=datetime.datetime(2008, 1, 1, 0, 0),
                 raisedAmt=50000, raisedCurrency='USD', round='seed'),
             Row(permalink='flypaper', company='Flypaper', numEmps=None, category='web',
                 city='Phoenix', state='AZ', fundedDate=datetime.datetime(2008, 2, 1, 0, 0),
                 raisedAmt=3000000, raisedCurrency='USD', round='a'),
             Row(permalink='chosenlist-com', company='ChosenList.com', numEmps=5, category='web',
                 city='Scottsdale', state='AZ', fundedDate=datetime.datetime(2008, 1, 25, 0, 0),
                 raisedAmt=233750, raisedCurrency='USD', round='angel'),
             Row(permalink='digg', company='Digg', numEmps=60, category='web',
                 city='San Francisco', state='CA',
                 fundedDate=datetime.datetime(2006, 12, 1, 0, 0), raisedAmt=8500000,
                 raisedCurrency='USD', round='b')]
        )

    def test_csv_read_with_given_schema(self):
        schema = StructType([
            StructField("permalink", StringType()),
            StructField("company", StringType()),
            StructField("numEmps", IntegerType()),
            StructField("category", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("fundedDate", DateType()),
            StructField("raisedAmt", IntegerType()),
            StructField("raisedCurrency", StringType()),
            StructField("round", StringType())
        ])
        df = spark.read.schema(schema).csv(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "data/fundings/"
            ),
            header=True
        )
        self.assertEqual(
            [Row(**r.asDict()) for r in df.collect()],
            [Row(permalink='mycityfaces', company='MyCityFaces', numEmps=7, category='web',
                 city='Scottsdale', state='AZ', fundedDate=datetime.date(2008, 1, 1),
                 raisedAmt=50000, raisedCurrency='USD', round='seed'),
             Row(permalink='flypaper', company='Flypaper', numEmps=None, category='web',
                 city='Phoenix', state='AZ', fundedDate=datetime.date(2008, 2, 1),
                 raisedAmt=3000000, raisedCurrency='USD', round='a'),
             Row(permalink='chosenlist-com', company='ChosenList.com', numEmps=5, category='web',
                 city='Scottsdale', state='AZ', fundedDate=datetime.date(2008, 1, 25),
                 raisedAmt=233750, raisedCurrency='USD', round='angel'),
             Row(permalink='digg', company='Digg', numEmps=60, category='web',
                 city='San Francisco', state='CA', fundedDate=datetime.date(2006, 12, 1),
                 raisedAmt=8500000, raisedCurrency='USD', round='b')]
        )
