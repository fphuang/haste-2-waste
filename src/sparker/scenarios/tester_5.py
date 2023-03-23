from pyspark.sql.types import StructField, StringType, IntegerType, StructType

from src.sparker.utils.spark_util import init_session

spark = init_session()


def try_read_1():
    # schema = StructType([
    #     StructField("name", StringType()),
    #     StructField("age", IntegerType()),
    #     StructField("address", StructType([
    #         StructField("city", StringType()),
    #         StructField("state", StringType())
    #     ]))
    # ])

    df = spark.read.option("multiline", "true") \
        .json("../datasets/multiline-zipcode.json")
    df.printSchema()
    df.show(truncate=False)


def try_to_pandas():
    data = [("James", "", "Smith", "36636", "M", 60000),
            ("Michael", "Rose", "", "40288", "M", 70000),
            ("Robert", "", "Williams", "42114", "", 400000),
            ("Maria", "Anne", "Jones", "39192", "F", 500000),
            ("Jen", "Mary", "Brown", "", "F", 0)]

    cols = ["first_name", "middle_name", "last_name", "dob", "gender", "salary"]
    df = spark.createDataFrame(data, cols)
    df.printSchema()
    df.show(truncate=False)


try_to_pandas()
