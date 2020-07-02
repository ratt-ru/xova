# -*- coding: utf-8 -*-

from africanus.averaging.dask import (time_and_channel,
                                      bda,
                                      tc_chan_metadata,
                                      tc_chan_average as tc_dask_chan_avg)
import dask.array as da
from dask.array.reductions import partial_reduce
from daskms import Dataset
import numpy as np

from xova.apps.xova.utils import id_full_like


def _safe_concatenate(args):
    # Handle singleton arg
    if not isinstance(args, list):
        args = [args]

    if isinstance(args[0], np.ndarray):
        return np.concatenate(args)
    elif isinstance(args[0], dict):
        d = args[0].copy()

        for a in args[1:]:
            n = len(d)
            d.update(("r%d" % (n+i), a["r%d" % i])
                     for i in range(1, len(a) + 1))

        assert len(d) == sum(len(a) for a in args)

        return d
    else:
        raise TypeError("Unhandled arg type %s" % type(args[0]))


def concatenate_row_chunks(array, group_every=4):
    """
    Parameters
    ----------
    array : :class:`dask.array.Array`
        dask array to average.
        First dimension must correspond to the MS 'row' dimension
    group_every : int
        Number of adjust dask array chunks to group together.
        Defaults to 4.

    When averaging, the output array's are substantially smaller, which
    can affect disk I/O since many small operations are submitted.
    This operation concatenates row chunks together so that more rows
    are submitted at once
    """

    # Single chunk already
    if len(array.chunks[0]) == 1:
        return array

    # Restrict the number of chunks to group to the
    # actual number of chunks in the array
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


def _flag_cats(flags):
    if isinstance(flags, np.ndarray):
        return flags[:, None, :, :]
    elif isinstance(flags, dict):
        return {k: v[:, None, :, :] for k, v in flags.items()}
    else:
        raise TypeError("Expected dict or ndarray")


def flag_categories(flag):
    # Single flag category, equal to flags
    return da.blockwise(_flag_cats, "rafc",
                        flag, "rfc",
                        new_axes={"a": 1},
                        dtype=flag.dtype)


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

    flag_cats = flag_categories(avg.flag)

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

    if hasattr(avg, "num_chan"):
        out_ds['NUM_CHAN'] = (("row",), avg.num_chan)

    if hasattr(avg, "decorr_chan_width"):
        out_ds['DECORR_CHAN_WIDTH'] = (("row",), avg.decorr_chan_width)

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


def average_main(main_ds, field_ds,
                 time_bin_secs, chan_bin_size,
                 fields, scan_numbers,
                 group_row_chunks, respect_flag_row,
                 viscolumn="DATA"):
    """
    Parameters
    ----------
    main_ds : list of Datasets
        Dataset containing Measurement Set columns.
        Should have a DATA_DESC_ID attribute.
    field_ds : list of Datasets
        Each Dataset corresponds to a row of the FIELD table.
    time_bin_secs : float
        Number of time bins to average together
    chan_bin_size : int
        Number of channels to average together
    fields : list
    scan_numbers : list
    group_row_chunks : int, optional
        Number of row chunks to concatenate together
    respect_flag_row : bool
        Respect FLAG_ROW instead of using FLAG
        for computing row flags.
    viscolumn: string
        name of column to average
    Returns
    -------
    avg
        tuple containing averaged data
    """
    output_ds = []

    for ds in main_ds:
        if fields and ds.FIELD_ID not in fields:
            continue

        if scan_numbers and ds.SCAN_NUMBER not in scan_numbers:
            continue

        if respect_flag_row is False:
            ds = ds.assign(FLAG_ROW=(("row",), ds.FLAG.data.all(axis=(1, 2))))

        dv = ds.data_vars

        # Default kwargs.
        kwargs = {'time_bin_secs': time_bin_secs,
                  'chan_bin_size': chan_bin_size,
                  'vis': dv[viscolumn].data}

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


def bda_average_main(main_ds,
                     field_ds,
                     ddid_ds,
                     spw_ds,
                     decorrelation,
                     fields,
                     scan_numbers,
                     group_row_chunks,
                     respect_flag_row,
                     viscolumn="DATA"):
    """
    Parameters
    ----------
    main_ds : list of Datasets
        Dataset containing Measurement Set columns.
        Should have a DATA_DESC_ID attribute.
    field_ds : list of Datasets
        Each Dataset corresponds to a row of the FIELD table.
    ddid_ds : Dataset
        Single Dataset containing the DATA_DESCRIPTION table.
    spw_ds : list of Datasets
        Each Dataset correspond to a row of the SPECTRAL_WINDOW table.
    decorrelation : float
        Decorrelation factor
    fields : list
    scan_numbers : list
    group_row_chunks : int, optional
        Number of row chunks to concatenate together
    respect_flag_row : bool
        Respect FLAG_ROW instead of using FLAG
        for computing row flags.
    viscolumn: string
        name of column to average
    Returns
    -------
    avg
        tuple containing averaged data
    """
    output_ds = []

    for ds in main_ds:
        if fields and ds.FIELD_ID not in fields:
            continue

        if scan_numbers and ds.SCAN_NUMBER not in scan_numbers:
            continue

        if respect_flag_row is False:
            ds = ds.assign(FLAG_ROW=(("row",), ds.FLAG.data.all(axis=(1, 2))))

        spw = ddid_ds.SPECTRAL_WINDOW_ID.values[ds.DATA_DESC_ID]
        ds = ds.assign(REF_FREQ=((), spw_ds[spw].REF_FREQUENCY.data[0]),
                       CHAN_WIDTH=(("chan",), spw_ds[spw].CHAN_WIDTH.data[0]))

        dv = ds.data_vars

        # Default kwargs.
        kwargs = {'decorrelation': decorrelation,
                  'vis': dv[viscolumn].data,
                  'format': 'ragged'}

        # Other columns with directly transferable names
        columns = ['FLAG_ROW', 'TIME_CENTROID', 'EXPOSURE', 'WEIGHT', 'SIGMA',
                   'UVW', 'FLAG', 'WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM',
                   'REF_FREQ', 'CHAN_WIDTH']

        for c in columns:
            try:
                kwargs[c.lower()] = dv[c].data
            except KeyError:
                pass

        # Set up the average operation
        avg = bda(dv['TIME'].data,
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
        chan_meta = tc_chan_metadata((), chan_arrays, chan_bin_size)
        # Average channel based data
        avg = tc_dask_chan_avg(chan_meta, chan_freq=chan_freq,
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


def _avg_wrapper(decorr_chan_width, nchan,
                 spw_num_chan,
                 spw_chan_width,
                 spw_chan_freq,
                 spw_effective_bw,
                 spw_resolution):

    spw_num_chan = spw_num_chan
    spw_chan_width = spw_chan_width[0]
    spw_chan_freq = spw_chan_freq[0]
    spw_effective_bw = spw_effective_bw[0]
    spw_resolution = spw_resolution[0]

    unum_chan, idx, inv = np.unique(nchan,
                                    return_index=True,
                                    return_inverse=True)
    print(unum_chan, decorr_chan_width[idx])

    return decorr_chan_width


def _channelisations(num_chan, decorr_chan_width, data_desc_id):
    num_chan, idx = np.unique(num_chan, return_index=True)
    return num_chan, decorr_chan_width[idx], data_desc_id[idx]


def _noop(x, keepdims, axis):
    return x


def combine(x, keepdims, axis):
    if isinstance(x, list):
        num_chan, decorr_cw, ddid = (np.concatenate(v) for v in zip(*x))
        return _channelisations(num_chan, decorr_cw, ddid)
    elif isinstance(x, tuple):
        return x
    else:
        raise TypeError("Unhandled combine type %s" % type(x))


def aggregate(x, keepdims, axis):
    num_chans, chan_width, ddid = (x if not isinstance(x, list) else
                                   combine(x, keepdims, axis))

    spws, idx = np.unique(np.stack([ddid, num_chans], axis=1),
                          axis=0, return_index=True)

    return {(d, nc): i for i, (d, nc) in enumerate(spws)}


def bda_average_spw(in_datasets, out_datasets, ddid_ds, spw_ds):
    """
    Parameters
    ----------
    in_datasets : list of Datasets
        list of Datasets
    out_datasets : list of Datasets
        list of Datasets
    ddid_ds : Dataset
        DATA_DESCRIPTION dataset
    spw_ds : list of Datasets
        list of Datasets, each describing a single Spectral Window

    Returns
    -------
    spw_ds : list of Datasets
        list of Datasets, each describing an averaged Spectral Window
    """

    channelisations = []

    for in_ds, out_ds in zip(in_datasets, out_datasets):
        spw_id = ddid_ds.SPECTRAL_WINDOW_ID.values[in_ds.DATA_DESC_ID]
        pol_id = ddid_ds.POLARIZATION_ID.values[in_ds.DATA_DESC_ID]
        spw = spw_ds[spw_id]

        assert len(spw.CHAN_WIDTH.data[0].chunks) == 1

        def fn(spw_id, ddid):
            return spw_id[0][ddid]

        spw_id = da.blockwise(fn, ("row",),
                              ddid_ds.SPECTRAL_WINDOW_ID.data, ("ddid",),
                              out_ds.DATA_DESC_ID.data, ("row",),
                              dtype=ddid_ds.SPECTRAL_WINDOW_ID.dtype)

        transform = da.blockwise(_channelisations, ("row",),
                                 out_ds.NUM_CHAN.data, ("row",),
                                 out_ds.DECORR_CHAN_WIDTH.data, ("row",),
                                 spw_id, ("row",),
                                 meta=np.empty((0,), dtype=np.object))

        result = da.reduction(transform,
                              chunk=_noop,
                              combine=combine,
                              aggregate=combine,
                              concatenate=False,
                              keepdims=True,
                              meta=np.empty((0,), dtype=np.object),
                              dtype=np.object)

        channelisations.append(result)

    result = da.reduction(da.concatenate(channelisations),
                          chunk=_noop,
                          combine=combine,
                          aggregate=aggregate,
                          concatenate=False,
                          meta=np.empty((0,), dtype=np.object),
                          dtype=np.object)

    def _map(ddid, num_chan, mapping):
        return np.array([mapping[(d, nc)] for d, nc
                        in zip(ddid, num_chan)])

    for i, out_ds in enumerate(out_datasets):
        ddid = da.blockwise(_map, ("row",),
                            out_ds.DATA_DESC_ID.data, ("row",),
                            out_ds.NUM_CHAN.data, ("row",),
                            result, (),
                            dtype=out_ds.DATA_DESC_ID.dtype)

        out_datasets[i] = out_ds.assign(DATA_DESC_ID=(("row",), ddid))

    return out_datasets
