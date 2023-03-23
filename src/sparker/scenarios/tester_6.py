from src.sparker.utils.spark_util import init_session
import pandas as pd
from pyspark.sql.functions import pandas_udf, ceil

spark = init_session()


def try_applyInPandas():
    data = [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)]
    cols = ["id", "v"]

    df = spark.createDataFrame(data, cols)

    def normalize(pdf: pd.DataFrame):
        v = pdf["v"]
        return pdf.assign(stats=(v - v.mean()) / v.std())

    df.groupBy("id").applyInPandas(
        normalize, schema="id long, v double, stats double"
    ).show()


try_applyInPandas()
