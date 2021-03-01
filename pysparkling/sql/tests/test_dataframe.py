import pytest

from pysparkling.sql import SparkSession


@pytest.fixture(name='spark')
def fixture_spark():
    return (
        SparkSession.builder
        .master("local")
        .appName("SparkByExamples.com")
        .getOrCreate()
    )


@pytest.fixture(name='df')
def fixture_df(spark):
    data = [
        ('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1),
    ]

    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
    return spark.createDataFrame(data=data, schema=columns)


def test_df_show(df, capsys):
    df.show()
    captured = capsys.readouterr()

    expected = ("""
+---------+----------+--------+----------+------+------+
|firstname|middlename|lastname|       dob|gender|salary|
+---------+----------+--------+----------+------+------+
|    James|          |   Smith|1991-04-01|     M|  3000|
|  Michael|      Rose|        |2000-05-19|     M|  4000|
|   Robert|          |Williams|1978-09-05|     M|  4000|
|    Maria|      Anne|   Jones|1967-12-01|     F|  4000|
|      Jen|      Mary|   Brown|1980-02-17|     F|    -1|
+---------+----------+--------+----------+------+------+
""").lstrip()

    assert captured.out == expected
