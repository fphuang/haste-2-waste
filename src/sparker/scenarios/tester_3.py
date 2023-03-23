from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from src.sparker.utils.spark_util import *
from pyspark.sql.functions import broadcast, udf


def try_ts():
    spark = init_session()
    rdd = spark.sparkContext.textFile("../datasets/test.txt")
    rdd2 = rdd.flatMap(lambda row: row.split(" "))
    rdd2 = rdd2.take(10)
    print(rdd2)


def try_flatmap():
    spark = init_session()
    rdd = spark.sparkContext.parallelize([[1, 2], [[34]], [56]])
    flattened_rdd = rdd.flatMap(lambda x: x)
    print(flattened_rdd.collect())


def word_count():
    spark = init_session()
    rdd = spark.sparkContext.textFile("../datasets/test.txt")
    rdd = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    rdd = rdd.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
    cache_rdd = rdd.cache()


def try_broadcast_vars():
    # Create a DataFrame
    spark = init_session()
    df = spark.createDataFrame([(1, "John"), (2, "Jane"), (3, "Bob")], ["id", "name"])

    # Create a lookup table as a DataFrame
    lookup_table = spark.createDataFrame([(1, "Male"), (2, "Female"), (3, "Male")], ["id", "gender"])

    # Broadcast the lookup table
    broadcast_table = broadcast(lookup_table)

    # Join the DataFrame with the broadcasted lookup table
    result = df.join(broadcast_table, ["id"])

    # Display the result
    result.show()


def try_repartition():
    spark = init_session()
    rdd = spark.sparkContext.parallelize(range(100))
    print(f"from local[3]: {rdd.getNumPartitions()}")

    rdd = spark.sparkContext.parallelize(range(1, 25), 6)
    print(rdd.keys().collect())


def try_broadcast_2():
    spark = init_session()
    states = {"NY": "New York", "CA": "California", "FL": "Florida"}
    broadcast_states = spark.sparkContext.broadcast(states)

    data = [
        ("James", "Smith", "USA", "CA"),
        ("Michael", "Rose", "USA", "NY"),
        ("Robert", "Williams", "USA", "CA"),
        ("Maria", "Jones", "USA", "FL")
    ]

    rdd = spark.sparkContext.parallelize(data)

    def state_convert(code: str):
        return broadcast_states.value[code]

    result = rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3])))
    print(result.collect())


def try_broadcast_3():
    spark = init_session()

    states = {"NY": "New York", "CA": "California", "FL": "Florida"}
    broadcast_states = spark.sparkContext.broadcast(states)

    data = [("James", "Smith", "USA", "CA"),
            ("Michael", "Rose", "USA", "NY"),
            ("Robert", "Williams", "USA", "CA"),
            ("Maria", "Jones", "USA", "FL")
            ]

    columns = ["firstname", "lastname", "country", "state"]
    df = spark.createDataFrame(data, columns)
    # df.printSchema()
    # df.show(truncate=False)

    def state_convert(code: str):
        return broadcast_states.value[code]

    my_udf = udf(lambda code: broadcast_states.value[code], StringType())

    # df = df.rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).toDF(columns)
    # df.show(truncate=False)

    df = df.withColumn("state", my_udf(df["state"]))
    df.show(truncate=False)

    df.select("firstname", "lastname", "country").show(truncate=False)

try_broadcast_3()
