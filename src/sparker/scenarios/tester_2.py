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
    df.show(n=10, truncate=False)


def try_csv():
    spark = init_session()
    df = spark.read.csv("../datasets/rdd/sample2.csv", header=True)
    df.printSchema()
    df.show(truncate=False)


try_csv()