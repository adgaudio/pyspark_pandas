import logging as _logging
log = _logging.getLogger('pyspark_pandas')

from dataframe_rdd import DataFrameRDD

import os.path as _p
import pkg_resources as _pkg_resources
__version__ = _pkg_resources.get_distribution(
    _p.basename(_p.dirname(_p.abspath(__file__)))).version
