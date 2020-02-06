# -*- coding: utf-8 -*-


from operator import getitem

import dask
import dask.array as da
import numpy as np

from loguru import logger


def _time_interval_sum(time, interval):
    # Work out unique times and their counts for this chunk
    utime, inv, counts = np.unique(time,
                                   return_inverse=True,
                                   return_counts=True)

    # Sum interval values for each unique time
    interval_sum = np.zeros(utime.shape[0], dtype=interval.dtype)
    np.add.at(interval_sum, inv, interval)
    return utime, interval_sum, counts


def _chunk(x_chunk, axis, keepdims):
    # Initial transform does nothing
    return x_chunk


def _time_int_combine(x_chunk, axis, keepdims):
    """ Intermediate reduction step """

    # x_chunk is a (label, interval_sum, count) tuple
    if isinstance(x_chunk, tuple):
        time, interval_sums, counts = x_chunk
    # x_chunk is a list of (label, interval_sum, count) tuples
    # Transpose into lists and concatenate them
    elif isinstance(x_chunk, list):
        time, interval_sums, counts = zip(*x_chunk)
        time = np.concatenate(time)
        interval_sums = np.concatenate(interval_sums)
        counts = np.concatenate(counts)
    else:
        raise TypeError("Unhandled x_chunk type %s" % type(x_chunk))

    utime, inv = np.unique(time, return_inverse=True)
    interval_sum = np.zeros(utime.shape[0], dtype=interval_sums.dtype)
    new_counts = np.zeros(utime.shape[0], dtype=counts.dtype)

    np.add.at(interval_sum, inv, interval_sums)
    np.add.at(new_counts, inv, counts)

    return utime, interval_sum, new_counts


def _time_int_agg(x_chunk, axis, keepdims):
    """ Final reduction step """
    utime, interval_sum, counts = _time_int_combine(x_chunk, axis, keepdims)
    return utime, counts, interval_sum / counts


class DatasetGrouper(object):
    def __init__(self, time_bin_secs, max_row_chunks):
        self.time_bin_secs = time_bin_secs
        self.max_row_chunks = max_row_chunks

    def group(self, ds_utime, ds_avg_intervals, ds_counts):
        (row_chunks,
         time_chunks,
         interval_secs) = self._group_into_time_bins(
                                        ds_utime,
                                        ds_avg_intervals,
                                        ds_counts)
        (row_chunks,
         time_chunks,
         interval_secs) = self._group_into_max_row_chunks(
                                        row_chunks,
                                        time_chunks,
                                        interval_secs)

        return row_chunks, time_chunks, interval_secs

    def _group_into_time_bins(self, ds_utime, ds_avg_intervals, ds_counts):
        # Produce row and time chunking strategies for each dataset
        ds_row_chunks = []
        ds_time_chunks = []
        ds_interval_secs = []

        it = zip(ds_utime, ds_avg_intervals, ds_counts)
        for di, (utime, avg_interval, counts) in enumerate(it):
            # Maintain row and time chunks for this dataset
            row_chunks = []
            time_chunks = []
            interval_secs = []

            # Start out with first entries
            bin_rows = counts[0]
            bin_times = 1
            bin_secs = avg_interval[0]

            dsit = enumerate(zip(utime[1:], avg_interval[1:], counts[1:]))
            for ti, (ut, avg_int, count) in dsit:
                if avg_int > self.time_bin_secs:
                    logger.warning("The average INTERVAL associated with "
                                   "unique time {:3f} in dataset {:d} "
                                   "is {:3f} but this exceeds the requested "
                                   "number of seconds in a time bin {:3f}s. "
                                   "Consider increasing --time-bin-secs",
                                   ut, di, avg_int, self.time_bin_secs)

                next_bin_secs = bin_secs + avg_int

                if next_bin_secs < self.time_bin_secs:
                    bin_secs = next_bin_secs
                    bin_rows += count
                    bin_times += 1
                # Otherwise finalize this bin and
                # start a new one with the counts
                # we were trying to add
                else:
                    row_chunks.append(bin_rows)
                    time_chunks.append(bin_times)
                    interval_secs.append(bin_secs)
                    bin_rows = count
                    bin_times = 1
                    bin_secs = avg_int

            # Finish any remaining bins
            if bin_rows > 0:
                assert bin_times > 0
                row_chunks.append(bin_rows)
                time_chunks.append(bin_times)
                interval_secs.append(bin_secs)

            row_chunks = tuple(row_chunks)
            time_chunks = tuple(time_chunks)
            interval_secs = tuple(interval_secs)
            ds_row_chunks.append(row_chunks)
            ds_time_chunks.append(time_chunks)
            ds_interval_secs.append(interval_secs)

        return ds_row_chunks, ds_time_chunks, ds_interval_secs

    def _group_into_max_row_chunks(self, row_chunks,
                                   time_chunks, interval_secs):
        final_row_chunks = []
        final_time_chunks = []
        final_interval_secs = []

        it = enumerate(zip(row_chunks, time_chunks, interval_secs))
        for di, (ds_row_chunks, ds_time_chunks, ds_interval_secs) in it:
            # Maintain row and time chunks for this dataset
            agg_row_chunks = []
            agg_time_chunks = []
            agg_interval_secs = []

            bin_rows = ds_row_chunks[0]
            bin_times = ds_time_chunks[0]
            bin_secs = ds_interval_secs[0]

            dsit = enumerate(zip(ds_row_chunks[1:],
                                 ds_time_chunks[1:],
                                 ds_interval_secs[1:]))
            for i, (rc, tc, ints) in dsit:
                next_bin_rows = bin_rows + rc

                if next_bin_rows < self.max_row_chunks:
                    bin_rows = next_bin_rows
                    bin_times += tc
                    bin_secs += ints
                else:
                    agg_row_chunks.append(bin_rows)
                    agg_time_chunks.append(bin_times)
                    agg_interval_secs.append(bin_secs)
                    bin_rows = rc
                    bin_times = tc
                    bin_secs = ints

            # Finish any remaining bins
            if bin_rows > 0:
                assert bin_times > 0
                agg_row_chunks.append(bin_rows)
                agg_time_chunks.append(bin_times)
                agg_interval_secs.append(bin_secs)

            final_row_chunks.append(agg_row_chunks)
            final_time_chunks.append(agg_time_chunks)
            final_interval_secs.append(agg_interval_secs)

        return final_row_chunks, final_time_chunks, final_interval_secs


def dataset_chunks(datasets, time_bin_secs, max_row_chunks):
    """
    Given ``max_row_chunks`` determine a chunking strategy
    for each dataset that prevents binning unique times in
    separate chunks.
    """
    # Calculate (utime, idx, counts) tuple for each dataset
    # then tranpose to get lists for each tuple entry
    if len(datasets) == 0:
        return (), ()

    utimes = []
    interval_avg = []
    counts = []
    monotonicity_checks = []

    for ds in datasets:
        # Compute unique times, their counts and interval sum
        # for each row chunk
        block_values = da.blockwise(_time_interval_sum, "r",
                                    ds.TIME.data, "r",
                                    ds.INTERVAL.data, "r",
                                    meta=np.empty((0,), dtype=np.object),
                                    dtype=np.object)

        # Reduce each row chunk's values
        reduction = da.reduction(block_values,
                                 chunk=_chunk,
                                 combine=_time_int_combine,
                                 aggregate=_time_int_agg,
                                 concatenate=False,
                                 split_every=16,
                                 meta=np.empty((0,), dtype=np.object),
                                 dtype=np.object)

        # Pull out the final unique times, counts and interval average
        utime = reduction.map_blocks(getitem, 0, dtype=ds.TIME.dtype)
        count = reduction.map_blocks(getitem, 1, dtype=np.int32)
        int_avg = reduction.map_blocks(getitem, 2, dtype=ds.INTERVAL.dtype)

        # Check monotonicity of TIME while we're at it
        is_monotonic = da.all(da.diff(ds.TIME.data) >= 0.0)

        utimes.append(utime)
        counts.append(count)
        interval_avg.append(int_avg)
        monotonicity_checks.append(is_monotonic)

    # Work out the unique times, average intervals for those times
    # and the frequency of those times
    (ds_utime,
     ds_avg_intervals,
     ds_counts,
     ds_monotonicity_checks) = dask.compute(utimes,
                                            interval_avg,
                                            counts,
                                            monotonicity_checks)

    if not all(ds_monotonicity_checks):
        raise ValueError("TIME is not monotonically increasing. "
                         "This is required.")

    grouper = DatasetGrouper(time_bin_secs, max_row_chunks)
    res = grouper.group(ds_utime, ds_avg_intervals, ds_counts)
    ds_row_chunks, ds_time_chunks,  ds_interval_secs = res

    logger.info("Dataset Chunking: (r)ow - (t)imes - (s)econds")

    it = zip(datasets, ds_row_chunks, ds_time_chunks, ds_interval_secs)
    for di, (ds, ds_rcs, ds_tcs, ds_int_secs) in enumerate(it):
        ds_rows = ds.dims['row']
        ds_crows = sum(ds_rcs)

        if not ds_rows == ds_crows:
            raise ValueError("Number of dataset rows %d "
                             "does not match the sum %d "
                             "of the row chunks %s"
                             % (ds_rows, ds_crows, ds_rcs))

        log_str = ", ".join("(%dr,%dt,%.1fs)" % (rc, tc, its)
                            for rc, tc, its
                            in zip(*(ds_rcs, ds_tcs, ds_int_secs)))

        logger.info("Dataset {d}: {s}", d=di, s=log_str)

    return ds_row_chunks, ds_time_chunks
