from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from src.sparker.utils.spark_util import init_session
from pytest import fixture

spark = init_session()


@fixture(scope="session")
def sc(request):
    sc = init_session()
    yield sc

    sc.stop()


def try_list(sc):
    rdd = sc.sparkContext.parallelize([i for i in range(1, 6)])
    assert rdd.count() == 5


def try_tuples():
    sc = init_session()
    data_list = [("Java", 2000), ("Python", 10000), ("Scala", 3000)]
    rdd = sc.sparkContext.parallelize(data_list)
    for item in rdd.collect():
        print(f"{item[0]}: {item[1]}")


def try_text():
    sc = init_session()
    rdd = sc.sparkContext.textFile("../datasets/rdd/ds_1.txt")
    print(rdd.first())


def try_csv():
    sc = init_session()
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zipcode", StringType(), True)
    ])

    rdd = sc.sparkContext.textFile("../datasets/rdd/sample2.csv")
    header = rdd.first()

    rows = rdd.filter(lambda line: line != header).map(lambda line: str(line).split(",")[:5])
    df = sc.createDataFrame(rows, schema)

    print("rdd data: ")
    for row in rdd.collect():
        print(row)

    print("DataFrame data: ")
    df.show(truncate=False)


try_csv()

