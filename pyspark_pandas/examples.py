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

    log.warn('rdd.take(1)')
    log.info(rdd.take(1))

    log.warn('rdd.mean()')
    log.info(rdd.mean())

    log.warn('rdd.percentileApprox(percentile=50)')
    log.info(rdd.percentileApprox(percentile=50))

    log.warn('rdd.get_nbytes()')
    log.info(rdd.get_nbytes())

    log.warn('rdd.get_nbytes(per_partition=True, per_frame=True)')
    log.info(rdd.get_nbytes(per_partition=True, per_frame=True))

    return rdd
