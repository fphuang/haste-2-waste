from pyspark.sql import SparkSession


def init_session(app_name: str = "zero_2_hero"):
    builder = SparkSession.builder\
        .appName(app_name).master("local[2]")

    return builder.getOrCreate()
