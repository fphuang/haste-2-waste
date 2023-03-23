import pyspark.pandas as ps
import numpy as np

# ps_df = ps.DataFrame(range(10))
# pd_df = ps_df.to_pandas()
# print(pd_df)
#
# ps_df = ps.from_pandas(pd_df).to_spark()
# ps_df.show()
#
# ps_df = ps_df.pandas_api()


ps.DataFrame(np.random.rand(100, 4), columns=list("abcd")).to_pandas().plot.area()
