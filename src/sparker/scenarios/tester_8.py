from src.sparker.utils.spark_util import init_session

spark = init_session()

data = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]


def to_df():
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF()
    df.printSchema()
    df.explain()
    df.groupBy("id").applyInPandas()
    df.show()


def create_df():
    df = spark.createDataFrame(data)
    df.show()


def to_df2():
    rdd = spark.sparkContext.parallelize([(x,) for x in range(10)])
    df = rdd.toDF()
    df.printSchema()
    df.show()


to_df()
