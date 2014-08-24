from __future__ import division

import numpy as np
import pandas as pd

from pyspark_pandas import log


def get_values_in_bounds(df, min_bound, max_bound, copy=False):
    """Get values within interval [min, max] (inclusive) for each column
    specified.  Where min or max not defined, assume no bound.

    Do not modify input data, but return a view on the dataframe.
    NaN safe.

    df - pandas.DataFrame with named columns
    min_bound - pandas.Series where index identifies columns
        and values are minimum allowed values (inclusive)
    max_bound - same as min_bound for maximum allowed values (inclusive)
    copy - (boolean) If True, return a copy of the data.  Otherwise, return a
        view on the given dataframe

    """
    _grid = np.ones(df.shape)
    if min_bound is not None:
        df = df[
            (df >= _grid * min_bound.T[df.columns].fillna(np.inf * -1).values)]
    if max_bound is not None:
        df = df[(df <= _grid * max_bound.T[df.columns].fillna(np.inf).values)]
    if copy:
        df = df.copy()
    return df


def _update_bounds_rv_and_k(min_bound, max_bound, rv, m_sum, n, k, pivot):
    """A function for percentile algorithm.

    For each column in the dataframes of the rdd,
    Mark as discarded either the elements > or < pivot, whichever is
    the smaller set.  If we discard elements > pivot, we need to
    reduce k.
    """
    mincols = m_sum.index[(m_sum > (n - k))]
    min_bound[mincols] = pivot[mincols]

    maxcols = m_sum.index[(m_sum < (n - k))]
    max_bound[maxcols] = pivot[maxcols]

    finished_cols = m_sum.index[
        (m_sum == (n - k))
        & (rv.isnull())]
    max_bound[finished_cols] = np.NaN
    min_bound[finished_cols] = np.NaN
    rv[finished_cols] = pivot[finished_cols]

    k[mincols] -= (n[mincols] - m_sum[mincols] - 1)


def rolling_avg(*mean_count_data):
    """Receive an arbitrary number of pairs of form (a, b), where:
        a = the mean (or a matrix of means)
        b = the count of elements considered in mean (or a matrix of counts)
    ie:
        >>> rolling_avg([1, 2], [3, 2])
        (2.0, 4)
    """
    # total number of elements in this mean
    mean_count_data = [(x.fillna(0), y.fillna(0)) for x, y in mean_count_data]
    newcount = sum(x[1] for x in mean_count_data)
    # sum of all elements considered
    newsum = sum(x[0] * x[1] for x in mean_count_data)
    newmean = newsum / newcount
    return (newmean, newcount)


class DataFrameRDD(object):

    def __init__(self, rdd, cache=False):
        log.debug('initializing DataFrameRDD')
        if cache:
            self.rdd = rdd.cache()
        else:
            self.rdd = rdd

    def __getattr__(self, key):
        """Hackily work around the object oriented pyspark design"""
        try:
            return getattr(self, key)
        except:
            return getattr(self.rdd, key)

    def max(self):
        def getmax(lst):
            return [pd.concat((df.max() for _, df in lst), axis=1).max(axis=1)]
        return (self.rdd
                .mapPartitions(getmax)
                .reduce(lambda x, y: pd.concat([x, y], axis=1).max(axis=1)))

    def mean(self, min_bound=None, max_bound=None):
        """Calculate the mean across all dataframes in the rdd

        Assume the rdd has the form: (unique_key, pandas.DataFrame(...))
        """
        log.info("Calculating mean")

        def mapper(key_df):
            df = get_values_in_bounds(key_df[1], min_bound, max_bound)
            return (df.mean(), df.count())
        rv = (
            self
            .rdd
            .map(mapper, True)
            .reduce(rolling_avg)
        )[0]
        rv.name = 'mean'
        return rv

    def percentile(self, percentile=50, maxiter=np.inf, max_frame_size=None):
        """
        Calculate the percentile (median by default) of each column in
        the distributed dataframes

        percentile - a number between [0, 100] inclusive
        maxiter - if given, stop after n iterations and raise an error
            (useful for debugging)
        max_frame_size - (int) as an optimization, the algorithm will attempt
            to merge all relevant elements into one large array and then
            calculate the median.  This number specifies the maximum num floats
            that can safely be stored in memory.

        Use this percentile algorithm when the entire column of a dataframe
        is too large to fit in memory.

        Assume rdd has the form: (unique_key, pandas.DataFrame(...))
        Assume each DataFrame has same columns
        Assume the index of each row in the rdd is globally unique across all
            frames in the rdd

        It does not estimate the percentile.
        If the percentile falls between two values,
            it gives you the lesser of the two values
        It safely handles na values
        """
        assert percentile > 0 and percentile < 100
        log.info('Calculating Percentile', extra=dict(percentile=percentile))
        rdd = self.rdd
        counts_bykey = self.countByKey()
        n = counts_bykey.sum()
        n.name = 'n, total count of values per column'
        k = percentile / 100 * n
        k.name = 'k, index location of percentile'
        if max_frame_size is None:
            max_frame_size = counts_bykey.sum(axis=1).max()
        max_bound = pd.Series(index=n.index).fillna(np.inf)
        max_bound.name = 'max_bound, limit above which median does not exist'
        min_bound = pd.Series(index=n.index).fillna(np.inf * -1)
        min_bound.name = 'min_bound, limit below which median does not exist'
        rv = pd.Series(index=n.index, name='rv, the percentile per column')
        nullcols = rv.index[rv.isnull()]

        _debugcnt = 0
        if max_frame_size > 0:
            loop = lambda: (
                counts_bykey[nullcols].sum(axis=1).sum() >= max_frame_size)
        else:
            loop = lambda: True
        while loop():
            log.debug('elements remaining to process: %s'
                      % counts_bykey[nullcols].sum(axis=1).sum())
            _debugcnt += 1
            if _debugcnt > maxiter:
                raise Exception('max allowed iterations reached')

            # Select a random value as the pivot
            pivot = self.sampleValue(counts_bykey=counts_bykey,
                                     min_bound=min_bound, max_bound=max_bound)
            # pivot = self.mean(min_bound=min_bound, max_bound=max_bound)
            # TODO: try: pivot = self.mean()  # per column mean on first try,
            # then next try shouldn't be a mean but some sort of gradient
            # function that approaches the median.  maybe similar to:
            # pivot = (oldmean/(oldmax-oldmin) + mean/(max-min)) / 2*(max-min)
            # TODO: have sampleValue return updated counts via with_counts=True
            log.debug('percentile: count values greater than pivot')

            def count_values_gt_than_pivot(key_df):
                key, df = key_df
                df = get_values_in_bounds(df, min_bound, max_bound)
                _grid = np.ones(df.shape) * pivot.T[df.columns].values
                return (df > _grid).sum()
            m = rdd.map(count_values_gt_than_pivot).collect()  # TODO:try sum()
            m_sum = sum(m).sort_index()
            m_sum.name = 'num_nodes > pivot'

            _update_bounds_rv_and_k(
                min_bound, max_bound, rv, m_sum, n, k, pivot)

            counts_bykey = self.countByKey(
                min_bound=min_bound, max_bound=max_bound)
            n = counts_bykey.sum()
            nullcols = rv.index[rv.isnull()]
            if not rv.isnull().any():
                return rv

        # merge remaining items and calculate percentile directly
        tmpdf = (
            rdd
            .map(lambda key_df: get_values_in_bounds(
                key_df[1][nullcols], min_bound, max_bound))
            .reduce(lambda x, y: pd.concat([x, y]))
        )
        for col in nullcols:
            rv[col] = pd.algos.kth_smallest(
                tmpdf[col].dropna().values, int(k[col]-1))
        return rv

    def percentileApprox(self, percentile=50, nbins=None):
        """Use a median of medians approach to calculate the percentile

        Exactly one of these should be defined:
            percentile - (optional) an int or array of ints
                in the range [0, 100] inclusive. Ignored if `nbins` is supplied.
            nbins - (optional) an integer number representing how many evenly
                spaced percentiles to calculate.
        """
        log.info("Executing percentileApprox", extra=dict(nbins=nbins))
        rdd = self.rdd
        percentile /= 100

        def get_percentiles(key_df):
            if nbins is None:
                q = [percentile]
            else:
                step_size = 1. / nbins
                q = np.arange(0, 1 + step_size, step_size)
            _, df = key_df
            return df.quantile(q)
        frame = pd.concat(rdd.map(get_percentiles, True).collect())
        # reduce the values from all distributed frames for each percentile
        log.debug('get median of each percentile bin')
        bins = frame.groupby(level=0).median()
        bins.index.name = 'percentileApprox'
        return bins

    def countByKey(self, axis=0, min_bound=None, max_bound=None, _rdd=None):
        """
        Return a dataframe of counts of non-na rows/cols for each distributed
        dataframe.  This does not count missing/na values.  Result frame has
        rows=keys of each distributed frame, columns=set of all columns across
        frames

        axis - 0=rows 1=columns
        """
        log.info("Executing countByKey")
        if _rdd is None:
            _rdd = self.rdd

        def func(df):
            return get_values_in_bounds(
                df, min_bound, max_bound).count(axis=axis)

        rv = pd.DataFrame.from_items(_rdd.mapValues(func).collect()).T
        rv.index.name = 'key'
        rv.columns.name = 'column'
        rv.sort_index(axis=1, inplace=True)
        return rv

    def sampleValue(self, counts_bykey=None, min_bound=None, max_bound=None):
        """Uniformly sample one value per column from the DataFrameRDD

        Assume the rdd has the form: (unique_key, pandas.DataFrame(...))

        counts_bykey - a list of (key, num_vals_per_column) tuples for every
            (key, dataframe) in the rdd you wish to sample from.  This is both
            an optimization and a way to sample from a specific subset
            of frames in the rdd.  By default, counts_bykey does not
            count null values.  If min_bound or max_bound are also
            supplied, these counts must consider the given bounds.

        min_bound or max_bound - (pandas.Series) if given, identifes bounds
            that the sampler must sample from.  Each Series given indexes the
            column names found in the distributed dataframes.
        """
        log.info("Executing sampleValue")
        if counts_bykey is None:
            counts_bykey = self.countByKey(
                min_bound=min_bound, max_bound=max_bound)
        # convert counts to a percent of the total per column across frames,
        # what pct of data points per (column, distributed frame) pair?
        # Then, structure these ratios for sampling via cumulative sum.
        cum_count_ratio_per_frame = np.cumsum(
            counts_bykey / counts_bykey.sum().astype('float64'))
        bad_counts = cum_count_ratio_per_frame.sum() > 0
        if not bad_counts.all():
            raise Exception(
                "You cannot sample from columns with no data."
                " Either your bounds are too strict or your"
                " counts_bykey are wrong.")
        sample = pd.Series(
            np.random.uniform(size=cum_count_ratio_per_frame.shape[1]),
            index=cum_count_ratio_per_frame.columns)

        def assign_key(col):
            """randomly select one distributed frame for each column"""
            return col.index[
                np.digitize([sample[col.name]], col.values)[0]]
        sample_keys = cum_count_ratio_per_frame.apply(assign_key)
        sample_keys.name = cum_count_ratio_per_frame.index.name

        def select_value(kv):
            """Select one random value from frame for this (column, frame) pair
            if the frame was chosen to sample from.
            (This function is mapped on the rdd)
            """
            key, df = kv
            # get columns that apply to this particular distributed frame
            cols = sample_keys.index[sample_keys == key]
            selected = df[cols]
            if selected.empty:
                return pd.Series()  # don't send the index
            # handle min and max bounds
            selected = get_values_in_bounds(selected, min_bound, max_bound)
            if selected.empty:
                return pd.Series()  # don't send the index
            rows = selected.apply(lambda ser: np.random.choice(ser.dropna()))
            if rows.empty:
                return pd.Series()  # don't send the index
            else:
                return rows
        rows = pd.concat(
            self.rdd.map(select_value).collect())
        rows.name = 'sample'
        return rows.sort_index()


def test_percentileApprox():
    raise NotImplemented()
    # bins = rdd.percentileApprox()
    # assert all(
    #     pd.algos.is_monotonic_float64(bins.values[:, col])[0]
    #     for col in range(bins.shape[1]))
    # pd.algos.is_monotonic_float64(bins.values[:, 1])
    # # find the count of values in each bin on all dataframes
    # # count the values in each bin on all distributed frames
    # log.debug('count values for each bin on each distributed dataframe')
    # def make_histogram(key_df):
    #     key, df = key_df
    #     hist = pd.DataFrame({
    #         col: np.histogram(df[col].values, bins[col].values)[0]
    #         for col in df.columns})
    #     return key, hist
    # hist = rdd.map(make_histogram, True).collect()
    # rv = pd.Panel.from_dict(dict(hist), orient='minor').to_frame()
    # rv.index.names = ['percentile', 'key']
    # rv.columns.name = 'columns'
    # return rv, bins
    # pass
    # # rdd = sc.parallelize([('key.' + str(i),
    #                        (makedf(1, index=np.arange(10))))
    #                        for i in range(5000)])
    # # rdd
    # # as you increase # shards, variance increases
    # # as you increase # data per shard, variance decreases


def test_percentile():
    raise NotImplemented()
    # set seed
    # rdd = sc.parallelize(
    # r2 = spark_percentile.DataFrameRDD(rdd) ; r2.sampleRow()
    pass  # TODO


def test_sampleValue():
    raise NotImplemented()


def test_countByKey():
    raise NotImplemented()
