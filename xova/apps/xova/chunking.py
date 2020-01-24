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
            if count > max_row_chunks:
                logger.warning("Unique time {:3f} occurred {:d} times "
                               "in dataset {:d} but this exceeds the "
                               "requested row chunks {:d}. "
                               "Consider increasing --row-chunks",
                               ut, count, di, max_row_chunks)

            if avg_int > time_bin_secs:
                logger.warning("The average INTERVAL associated with "
                               "unique time {:3f} in dataset {:d} "
                               "is {:3f} but this exceeds the requested "
                               "number of seconds in a time bin {:3f}s. "
                               "Consider increasing --time-bin-secs",
                               ut, di, avg_int, time_bin_secs)

            next_rows = bin_rows + count

            # If we're still within the number of rows for this bin
            # keep going
            if next_rows < max_row_chunks:
                bin_rows = next_rows
                bin_times += 1
                bin_secs += avg_int
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

    return ds_row_chunks, ds_time_chunks, ds_interval_secs
