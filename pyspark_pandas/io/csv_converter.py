import io
import pandas as pd

from pyspark_pandas import log
from pyspark_pandas.dataframe_rdd import DataFrameRDD


def read_whole_csvs(sc, hadoop_fp):
    log.debug(
        "Loading DataFrameRDD from csvs", extra=dict(hadoop_fp=hadoop_fp))

    def to_dataframe(fp_data):
        fp, data = fp_data
        # Convert data to pandas.DataFrame
        buf = io.StringIO()
        buf.write(data)
        buf.seek(0)
        df = pd.read_csv(buf)
        return (fp, df)

    rdd = (
        sc
        .wholeTextFiles(hadoop_fp)
        .map(to_dataframe, True)
    )
    return DataFrameRDD(rdd)
