# -*- coding: utf-8 -*-

from africanus.averaging.dask import (time_and_channel,
                                      chan_metadata,
                                      chan_average as dask_chan_avg)
import dask.array as da
from dask.array.reductions import partial_reduce
from daskms import Dataset
import numpy as np

from xova.apps.xova.utils import id_full_like


def _safe_concatenate(*args):
    # Handle list with single numpy array case,
    # tuple unpacking fails on it
    if len(args) == 1 and isinstance(args[0], np.ndarray):
        return args[0]

    return np.concatenate(*args)


def concatenate_row_chunks(array, group_every=1000):
    """
    When averaging, the output array's are substantially smaller, which
    can affect disk I/O since many small operations are submitted.
    This operation concatenates row chunks together so that more rows
    are submitted at once
    """

    # Single chunk already
    if len(array.chunks[0]) == 1:
        return array

    group_every = min(len(array.chunks[0]), group_every)
    data = partial_reduce(_safe_concatenate, array,
                          split_every={0: group_every},
                          reduced_meta=None, keepdims=True)

    # NOTE(sjperkins)
    # partial_reduce sets the number of rows in each chunk
    # to 1, which is untrue. Correctly set the row chunks to nan,
    # steal the graph and recreate the array
    row_chunks = tuple(np.nan for _ in data.chunks[0])
    chunks = (row_chunks,) + data.chunks[1:]
    graph = data.__dask_graph__()

    return da.Array(graph, data.name, chunks, dtype=data.dtype)


def output_dataset(avg, field_id, data_desc_id, scan_number,
                   group_row_chunks):
    """
    Parameters
    ----------
    avg : namedtuple
        Result of :func:`average`
    field_id : int
        FIELD_ID for this averaged data
    data_desc_id : int
        DATA_DESC_ID for this averaged data
    scan_number : int
        SCAN_NUMBER for this averaged data

    Returns
    -------
    Dataset
        Dataset containing averaged data
    """
    # Create ID columns
    field_id = id_full_like(avg.time, fill_value=field_id)
    data_desc_id = id_full_like(avg.time, fill_value=data_desc_id)
    scan_number = id_full_like(avg.time, fill_value=scan_number)

    # Single flag category, equal to flags
    flag_cats = avg.flag[:, None, :, :]

    out_ds = {
        # Explicitly zero these columns? But this happens anyway
        # "ARRAY_ID": (("row",), zeros),
        # "OBSERVATION_ID": (("row",), zeros),
        # "PROCESSOR_ID": (("row",), zeros),
        # "STATE_ID": (("row",), zeros),

        "ANTENNA1": (("row",), avg.antenna1),
        "ANTENNA2": (("row",), avg.antenna2),
        "DATA_DESC_ID": (("row",), data_desc_id),
        "FIELD_ID": (("row",), field_id),
        "SCAN_NUMBER": (("row",), scan_number),

        "FLAG_ROW": (("row",), avg.flag_row),
        "FLAG_CATEGORY": (("row", "flagcat", "chan", "corr"), flag_cats),
        "TIME": (("row",), avg.time),
        "INTERVAL": (("row",), avg.interval),
        "TIME_CENTROID": (("row",), avg.time_centroid),
        "EXPOSURE": (("row",), avg.exposure),
        "UVW": (("row", "[uvw]"), avg.uvw),
        "WEIGHT": (("row", "corr"), avg.weight),
        "SIGMA": (("row", "corr"), avg.sigma),

        "DATA": (("row", "chan", "corr"), avg.vis),
        "FLAG": (("row", "chan", "corr"), avg.flag),
    }

    # Add optionally averaged columns columns
    if avg.weight_spectrum is not None:
        out_ds['WEIGHT_SPECTRUM'] = (("row", "chan", "corr"),
                                     avg.weight_spectrum)

    if avg.sigma_spectrum is not None:
        out_ds['SIGMA_SPECTRUM'] = (("row", "chan", "corr"),
                                    avg.sigma_spectrum)

    # Concatenate row chunks together
    if group_row_chunks > 1:
        grc = group_row_chunks
        out_ds = {k: (dims, concatenate_row_chunks(data, group_every=grc))
                  for k, (dims, data) in out_ds.items()}

    return Dataset(out_ds)


def average_main(main_ds, time_bin_secs, chan_bin_size,
                 group_row_chunks, respect_flag_row):
    """
    Parameters
    ----------
    main_ds : list of Datasets
        Dataset containing Measurement Set columns.
        Should have a DATA_DESC_ID attribute.
    time_bin_secs : float
        Number of time bins to average together
    chan_bin_size : int
        Number of channels to average together
    group_row_chunks : int, optional
        Number of row chunks to concatenate together
    respect_flag_row : bool
        Respect FLAG_ROW instead of using FLAG
        for computing row flags.

    Returns
    -------
    avg
        tuple containing averaged data
    """
    # Get the appropriate spectral window and polarisation Dataset
    # Must have a single DDID table

    output_ds = []

    for ds in main_ds:
        if respect_flag_row is False:
            ds = ds.assign(FLAG_ROW=(("row",), ds.FLAG.data.all(axis=(1, 2))))

        dv = ds.data_vars

        # Default kwargs
        kwargs = {'time_bin_secs': time_bin_secs,
                  'chan_bin_size': chan_bin_size,
                  'vis': dv['DATA'].data}

        # Other columns with directly transferable names
        columns = ['FLAG_ROW', 'TIME_CENTROID', 'EXPOSURE', 'WEIGHT', 'SIGMA',
                   'UVW', 'FLAG', 'WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM']

        for c in columns:
            try:
                kwargs[c.lower()] = dv[c].data
            except KeyError:
                pass

        # Set up the average operation
        avg = time_and_channel(dv['TIME'].data,
                               dv['INTERVAL'].data,
                               dv['ANTENNA1'].data,
                               dv['ANTENNA2'].data,
                               **kwargs)

        output_ds.append(output_dataset(avg,
                                        ds.FIELD_ID,
                                        ds.DATA_DESC_ID,
                                        ds.SCAN_NUMBER,
                                        group_row_chunks))

    return output_ds


def average_spw(spw_ds, chan_bin_size):
    """
    Parameters
    ----------
    spw_ds : list of Datasets
        list of Datasets, each describing a single Spectral Window
    chan_bin_size : int
        Number of channels in an averaging bin

    Returns
    -------
    spw_ds : list of Datasets
        list of Datasets, each describing an averaged Spectral Window
    """

    new_spw_ds = []

    for r, spw in enumerate(spw_ds):
        # Get the dataset variables as a mutable dictionary
        dv = dict(spw.data_vars)

        # Extract arrays we wish to average
        chan_freq = dv['CHAN_FREQ'].data[0]
        chan_width = dv['CHAN_WIDTH'].data[0]
        effective_bw = dv['EFFECTIVE_BW'].data[0]
        resolution = dv['RESOLUTION'].data[0]

        # Construct channel metadata
        chan_arrays = (chan_freq, chan_width, effective_bw, resolution)
        chan_meta = chan_metadata((), chan_arrays, chan_bin_size)
        # Average channel based data
        avg = dask_chan_avg(chan_meta, chan_freq=chan_freq,
                            chan_width=chan_width,
                            effective_bw=effective_bw,
                            resolution=resolution,
                            chan_bin_size=chan_bin_size)

        num_chan = da.full((1,), avg.chan_freq.shape[0], dtype=np.int32)

        # These columns change, re-create them
        dv['NUM_CHAN'] = (("row",), num_chan)
        dv['CHAN_FREQ'] = (("row", "chan"), avg.chan_freq[None, :])
        dv['CHAN_WIDTH'] = (("row", "chan"), avg.chan_width[None, :])
        dv['EFFECTIVE_BW'] = (("row", "chan"), avg.effective_bw[None, :])
        dv['RESOLUTION'] = (("row", "chan"), avg.resolution[None, :])

        # But re-use all the others
        new_spw_ds.append(Dataset(dv))

    return new_spw_ds
