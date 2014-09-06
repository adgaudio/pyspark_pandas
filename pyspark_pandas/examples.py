from pyspark_pandas import log, DataFrameRDD

import pyspark
import numpy as np
import pandas as pd


def make_frame(i=0, index=list('abcdefghij'), columns=list('ABCDEFGHIJ')):
    log.info("Creating a random dataframe")
    return i + pd.DataFrame(
        np.random.randn(len(index), len(columns)),
        index=index, columns=columns)


def spark_context():
    log.info("initializing a pyspark.SparkContext for testing")
    try:
        return pyspark.SparkContext('local[4]', 'pyspark_pandas_demo')
    except ValueError:
        log.warn("Another Spark Context is already active.  Using that one")
        return pyspark.SparkContext._active_spark_context


def get_rdd(seed=0):
    np.random.seed(seed)
    sc = spark_context()
    rdd = sc.parallelize(
        [('key.%s' % key, make_frame()) for key in range(10)], 4)
    rdd = DataFrameRDD(rdd)
    return rdd


def demo():
    rdd = get_rdd()

    print('\nexecuting: rdd.take(1)')
    print(rdd.take(1))

    print('\nexecuting: rdd.mean()')
    print(rdd.mean())

    print('\nexecuting: rdd.percentileApprox(percentile=50)')
    print(rdd.percentileApprox(percentile=50))

    print('\nexecuting: rdd.get_nbytes()')
    print(rdd.get_nbytes())

    print('\nexecuting: rdd.get_nbytes(per_partition=True, per_frame=True)')
    print(rdd.get_nbytes(per_partition=True, per_frame=True))

    return rdd
