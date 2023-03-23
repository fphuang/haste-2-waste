from src.sparker.utils.spark_util import init_session


def try_list():
    data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
            ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
            ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
            ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
            ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
            ]

    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
    spark = init_session()
    df = spark.createDataFrame(data=data, schema=columns)
    df.printSchema()
    # df.show(n=10, truncate=False)

    print("the number of partitions: ", df.rdd.getNumPartitions())
    df = df.repartition(4, "firstname")
    print(f"{df.rdd.getNumPartitions()}")
    df.write.mode("overwrite").parquet("../datasets/employees")


def try_csv():
    spark = init_session()
    df = spark.read.csv("../datasets/rdd/sample2.csv", header=True)
    df.printSchema()
    df.show(truncate=False)
    df = df.dropna()
    df.show(truncate=False)


def try_read_parquet():
    spark = init_session()
    files = ["../datasets/employees/part-00000-21a0955a-bd43-4174-b5a7-e44523ddc4ad-c000.snappy.parquet",
             "../datasets/employees/part-00001-21a0955a-bd43-4174-b5a7-e44523ddc4ad-c000.snappy.parquet",
             "../datasets/employees/part-00002-21a0955a-bd43-4174-b5a7-e44523ddc4ad-c000.snappy.parquet",
             "../datasets/employees/part-00003-21a0955a-bd43-4174-b5a7-e44523ddc4ad-c000.snappy.parquet",
             "../datasets/employees/"]

    for file in files:
        df = spark.read.parquet(file)
        df.show(truncate=False)


def try_empty():
    spark = init_session()
    rdd = spark.sparkContext.emptyRDD()
    print(rdd.getNumPartitions())

    rdd = rdd.repartition(10)
    print(rdd.getNumPartitions())

    rdd = rdd.coalesce(8, shuffle=False)
    print(rdd.getNumPartitions())

    
try_empty()
