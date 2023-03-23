from src.sparker.utils.spark_util import init_session

spark = init_session()
accuSum = spark.sparkContext.accumulator(0)


def try_accu_1():
    rdd = spark.sparkContext.parallelize([i for i in range(20)])
    rdd.foreach(countFunc)
    print(accuSum.value)


def countFunc(x):
    global accuSum
    accuSum += x


try_accu_1()
