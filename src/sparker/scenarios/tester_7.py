from pyspark.sql.types import StructType, StringType, StructField, IntegerType

from src.sparker.utils.spark_util import init_session

spark = init_session()

schema = StructType(
    [StructField('name',
                 StructType([
                     StructField('firstname', StringType(), True),
                     StructField('middlename', StringType(), True),
                     StructField('lastname', StringType(), True)
                 ]),
                 False),
     StructField('age', IntegerType(), True),
     StructField('address', StringType(), True)
     ])


def create_empty_df():
    df = spark.createDataFrame([], schema=schema)
    df.printSchema()


def create_empty_rdd():
    rdd = spark.sparkContext.emptyRDD()
    print(rdd.collect())


create_empty_df()
