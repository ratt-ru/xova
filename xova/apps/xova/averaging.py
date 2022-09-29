# -*- coding: utf-8 -*-

from operator import getitem

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


def row_concatenate(array, group_every=4):
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
                          reduced_meta=None,
                          keepdims=True,
                          dtype=array.dtype)

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

    zeros = id_full_like(avg.antenna1, fill_value=0)

    out_ds = {
        # Explicitly zero these columns? But this happens anyway
        "ARRAY_ID": (("row",), zeros),
        "OBSERVATION_ID": (("row",), zeros),
        "PROCESSOR_ID": (("row",), zeros),
        "STATE_ID": (("row",), zeros),

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
        "FLAG": (("row", "chan", "corr"), avg.flag),
    }

    if avg.visibilities is not None:
        if type(avg.visibilities) is dict:
            for column, data in avg.visibilities.items():
                out_ds[column] = (("row", "chan", "corr"), data)
        elif isinstance(avg.visibilities, da.Array):
            out_ds["DATA"] = (("row", "chan", "corr"), avg.visibilities)
        else:
            raise TypeError(f"Unknown visibility type {type(avg.visibilities)}")

    if hasattr(avg, "offsets"):
        num_chan = da.map_blocks(np.diff, avg.offsets)
        out_ds['NUM_CHAN'] = (("row",), num_chan)

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
        out_ds = {k: (dims, row_concatenate(data, group_every=grc))
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
                  'chan_bin_size': chan_bin_size}

        from_column, to_column = zip(*viscolumn.items())

        try:
            kwargs['visibilities'] = tuple(dv[dc].data for dc in from_column)
        except KeyError:
            raise ValueError("Visibility column %s not present" % viscolumn)

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

        avg_dict = avg._asdict()
        avg_dict['visibilities'] = dict(zip(to_column, avg.visibilities))
        avg = avg.__class__(*avg_dict.values())

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
                     args):
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
    args : object
        Application arguments
    Returns
    -------
    avg
        tuple containing averaged data
    """
    output_ds = []

    for ds in main_ds:
        if args.fields and ds.FIELD_ID not in args.fields:
            continue

        if args.scan_numbers and ds.SCAN_NUMBER not in args.scan_numbers:
            continue

        if args.respect_flag_row is False:
            ds = ds.assign(FLAG_ROW=(("row",), ds.FLAG.data.all(axis=(1, 2))))

        spw = ddid_ds.SPECTRAL_WINDOW_ID.values[ds.DATA_DESC_ID]
        ds = ds.assign(CHAN_WIDTH=(("chan",), spw_ds[spw].CHAN_WIDTH.data[0]),
                       CHAN_FREQ=(("chan",), spw_ds[spw].CHAN_FREQ.data[0]))

        dv = ds.data_vars

        # Default kwargs.
        kwargs = {'decorrelation': args.decorrelation,
                  'max_fov': args.max_fov,
                  'time_bin_secs': args.time_bin_secs,
                  'min_nchan': args.min_nchan,
                  'format': 'ragged'}

        from_cols, to_cols = zip(*args.data_column.items())

        try:
            kwargs['visibilities'] = tuple(dv[fc].data for fc in from_cols)
        except KeyError:
            raise ValueError(f"Some of {from_cols} are not present")

        # Other columns with directly transferable names
        columns = ['FLAG_ROW', 'TIME_CENTROID', 'EXPOSURE', 'WEIGHT', 'SIGMA',
                   'UVW', 'FLAG', 'WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM',
                   'CHAN_WIDTH', 'CHAN_FREQ']

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

        avg_dict = avg._asdict()
        avg_dict['visibilities'] = dict(zip(to_cols, avg.visibilities))
        avg = avg.__class__(**avg_dict)

        output_ds.append(output_dataset(avg,
                                        ds.FIELD_ID,
                                        ds.DATA_DESC_ID,
                                        ds.SCAN_NUMBER,
                                        args.group_row_chunks))

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


def _channelisations(ddid, num_chan, spw_id, pol_id):
    ddid_chan_map = np.stack([ddid,
                              spw_id[0][ddid],
                              pol_id[0][ddid],
                              num_chan], axis=1)

    return np.unique(ddid_chan_map, axis=0)


def _noop(x, keepdims, axis):
    return x


def combine(x, keepdims, axis):
    if isinstance(x, list):
        return np.unique(np.concatenate(x), axis=0)
    elif isinstance(x, np.ndarray):
        return x
    else:
        raise TypeError("Unhandled combine type %s" % type(x))


def aggregate(x, keepdims, axis):
    return (x if not isinstance(x, list) else
            combine(x, keepdims, axis))


def ddid_and_spw_factory(chan_freqs, chan_widths,
                         ref_freqs, meas_freq_refs,
                         ddid_chan_map):
    """
    Parameters
    ----------
    chan_freqs : tuple of :class:`numpy.ndarray`
        Tuple of channel frequencies for each SPW
    chan_widths : tuple of :class:`numpy.ndarray`
        Tuple of channel widths for each SPW
    ref_freqs : tuple of :class:`numpy.ndarray`
        Tuple of reference frequencies for each SPW
    meas_freq_refs : tuple of :class:`numpy.ndarray`
        Tuple of Measure Frequency References for each SPW
    ddid_chan_map : dict
        Data Descriptor ID channelisation map

    Returns
    -------
    rcw : dict
        channel ("CHAN_WIDTH") width row entries
        suitable for putvarcol
    rcf : dict
        channel frequency ("CHAN_FREQ") row entries
        suitable for putvarcol
    rnc : dict
        number of channel ("NUM_CHAN") row entries
        suitable for putvarcol
    rrf : dict
        reference frequency ("REF_FREQUENCY") row entries
        suitable for putvarcol
    rtbw : dict
        total bandwidth ("TOTAL_BANDWIDTH") row entries
        suitable for putvarcol
    spectral_window_ids : :class:`np.ndarray`
        new SPECTRAL_WINDOW_ID values for insertion into
        DATA_DESCRIPTION subtable.
    polarization_ids : :class:`np.ndarray`
        new POLARIZATION_ID values for insertion into
        DATA_DESCRIPTION subtable
    new_ddid_map : dict
        {(old_ddid, num_chan): new_ddid} map, suitable
        for creating new DATA_DESC_ID arrays.
    """
    # Find unique channelisations per SPW
    spw_chan_map = ddid_chan_map[:, (1, 3)]
    uspw_chan_map, idx, inv = np.unique(spw_chan_map,
                                        return_index=True,
                                        return_inverse=True,
                                        axis=0)

    # Sanity checks
    if not (all(cf.size >= 1 for cf in chan_freqs) and all(cw.size >= 1 for cw in chan_widths)):
        raise ValueError("No SPW may be empty")
    if not all(np.all(np.diff(cf) > 0.0 if cf.size > 1 else [True]) for cf in chan_freqs):
        raise ValueError("Decreasing CHAN_FREQ unsupported")
    if not all(np.all(cw[0] == c for c in (cw if cw.size > 1 else cw.reshape(1))) for cw in chan_widths):
        raise ValueError("Heterogenous CHAN_WIDTH unsupported")

    rowid = 0
    rcw = {}    # CHAN_WIDTH
    rcf = {}    # CHAN_FREQ
    rnc = {}    # NUM_CHAN
    rrf = {}    # REF_FREQUENCY
    rtbw = {}   # TOTAL_BANDWIDTH
    rmrf = {}   # MEAS_FREQ_REF

    # Generate SPW's for each channelisation
    for spw, nchan in uspw_chan_map:
        cf = chan_freqs[spw] if chan_freqs[spw].size > 1 else chan_freqs[spw].reshape(1)
        cw = chan_widths[spw] if chan_widths[spw].size > 1 else chan_widths[spw].reshape(1)
        start = cf[0] - cw[0] / 2
        end = cf[-1] + cw[-1] / 2

        bandwidth = cw.sum()  # Maybe TOTAL_BANDWIDTH?
        cw = np.full(nchan, bandwidth / nchan)
        cf = np.linspace(start + cw[0] / 2,
                         end - cw[-1] / 2,
                         nchan)

        key = "r%d" % rowid
        rcw[key] = cw[None, :]
        rcf[key] = cf[None, :]
        rnc[key] = np.array([nchan])
        rrf[key] = np.array([ref_freqs[spw]])
        rmrf[key] = np.array([meas_freq_refs[spw]])
        rtbw[key] = np.array([bandwidth])
        rowid += 1

    # Create the SPECTRAL_WINDOW_ID, POLARIZATION_ID and
    # {(old_ddid, num_chan): new_ddid} mapping
    spectral_window_ids = []
    polarization_ids = []
    new_ddid_map = {}

    it = enumerate(np.concatenate((ddid_chan_map, inv[:, None]), axis=1))
    for new_ddid, (old_ddid, _, pol_id, nchan, spw_id) in it:
        spectral_window_ids.append(spw_id)
        polarization_ids.append(pol_id)
        new_ddid_map[(old_ddid, nchan)] = new_ddid

    spectral_window_ids = np.asarray(spectral_window_ids)
    polarization_ids = np.asarray(polarization_ids)

    return (rcf, rcw, rnc, rrf, rmrf, rtbw,
            spectral_window_ids, polarization_ids,
            new_ddid_map)


def _new_ddids(old_ddid, num_chan, ddid_map):
    ddid_map = ddid_map[0]
    return np.asarray([ddid_map[(d, nc)] for d, nc in zip(old_ddid, num_chan)])


def bda_average_spw(out_datasets, ddid_ds, spw_ds):
    """
    Parameters
    ----------
    out_datasets : list of Datasets
        list of Datasets
    ddid_ds : Dataset
        DATA_DESCRIPTION dataset
    spw_ds : list of Datasets
        list of Datasets, each describing a single Spectral Window

    Returns
    -------
    output_ds : list of Datasets
        list of Datasets
    spw_ds : list of Datasets
        list of Datasets, each describing an averaged Spectral Window
    """

    channelisations = []

    # Over the entire set of datasets, determine the complete
    # set of channelisations, per input DDID and
    # reduce down to a single object
    for out_ds in out_datasets:
        transform = da.blockwise(_channelisations, ("row",),
                                 out_ds.DATA_DESC_ID.data, ("row",),
                                 out_ds.NUM_CHAN.data, ("row",),
                                 ddid_ds.SPECTRAL_WINDOW_ID.data, ("ddid",),
                                 ddid_ds.POLARIZATION_ID.data, ("ddid",),
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

    # Final reduction object, note the aggregate method
    # which generates the mapping
    ddid_chan_map = da.reduction(da.concatenate(channelisations),
                                 chunk=_noop,
                                 combine=combine,
                                 aggregate=aggregate,
                                 concatenate=False,
                                 keepdims=False,
                                 meta=np.empty((), dtype=np.object),
                                 dtype=np.object)

    def _squeeze_tuplify(*args):
        return tuple(a.squeeze() for a in args)

    chan_freqs = da.blockwise(_squeeze_tuplify, ("row", "chan"),
                              *(a for spw in spw_ds for a
                                in (spw.CHAN_FREQ.data, ("row", "chan"))),
                              concatenate=False,
                              align_arrays=False,
                              adjust_chunks={"chan": lambda c: np.nan},
                              meta=np.empty((0, 0), dtype=np.object))

    chan_widths = da.blockwise(_squeeze_tuplify, ("row", "chan"),
                               *(a for spw in spw_ds for a
                                 in (spw.CHAN_WIDTH.data, ("row", "chan"))),
                               concatenate=False,
                               align_arrays=False,
                               adjust_chunks={"chan": lambda c: np.nan},
                               meta=np.empty((0, 0), dtype=np.object))

    ref_freqs = da.blockwise(_squeeze_tuplify, ("row",),
                             *(a for spw in spw_ds for a
                               in (spw.REF_FREQUENCY.data, ("row",))),
                             concatenate=False,
                             align_arrays=False,
                             meta=np.empty((0,), dtype=np.object))

    meas_freq_refs = da.blockwise(_squeeze_tuplify, ("row",),
                                  *(a for spw in spw_ds for a
                                    in (spw.REF_FREQUENCY.data, ("row",))),
                                  concatenate=False,
                                  align_arrays=False,
                                  meta=np.empty((0,), dtype=np.object))

    result = da.blockwise(ddid_and_spw_factory, ("row", "chan"),
                          chan_freqs, ("row", "chan"),
                          chan_widths, ("row", "chan"),
                          ref_freqs, ("row",),
                          meas_freq_refs, ("row",),
                          ddid_chan_map, (),
                          meta=np.empty((0, 0), dtype=np.object))

    # There should only be one chunk
    assert result.npartitions == 1

    chan_freq = da.blockwise(getitem, ("row", "chan"),
                             result, ("row", "chan"),
                             0, None,
                             dtype=np.float64)

    chan_width = da.blockwise(getitem, ("row", "chan"),
                              result, ("row", "chan"),
                              1, None,
                              dtype=np.float64)

    num_chan = da.blockwise(lambda d, i: d[0][i], ("row",),
                            result, ("row", "chan"),
                            2, None,
                            dtype=np.int32)

    ref_freq = da.blockwise(lambda d, i: d[0][i], ("row",),
                            result, ("row", "chan"),
                            3, None,
                            dtype=np.float64)

    meas_freq_refs = da.blockwise(lambda d, i: d[0][i], ("row",),
                                  result, ("row", "chan"),
                                  4, None,
                                  dtype=np.float64)

    total_bw = da.blockwise(lambda d, i: d[0][i], ("row",),
                            result, ("row", "chan"),
                            5, None,
                            dtype=np.float64)

    spectral_window_id = da.blockwise(lambda d, i: d[0][i], ("row",),
                                      result, ("row", "chan"),
                                      6, None,
                                      dtype=np.int32)

    polarization_id = da.blockwise(lambda d, i: d[0][i], ("row",),
                                   result, ("row", "chan"),
                                   7, None,
                                   dtype=np.int32)

    ddid_map = da.blockwise(lambda d, i: d[0][i], ("row",),
                            result, ("row", "chan"),
                            8, None,
                            dtype=np.int32)

    for o, out_ds in enumerate(out_datasets):
        data_desc_id = da.blockwise(_new_ddids, ("row",),
                                    out_ds.DATA_DESC_ID.data, ("row",),
                                    out_ds.NUM_CHAN.data, ("row",),
                                    ddid_map, ("ddid",),
                                    dtype=out_ds.DATA_DESC_ID.dtype)

        dv = dict(out_ds.data_vars)
        dv["DATA_DESC_ID"] = (("row",), data_desc_id)
        del dv["NUM_CHAN"]
        del dv["DECORR_CHAN_WIDTH"]

        out_datasets[o] = Dataset(dv, out_ds.coords, out_ds.attrs)

    out_spw_ds = Dataset({
        "CHAN_FREQ": (("row", "chan"), chan_freq),
        "CHAN_WIDTH": (("row", "chan"), chan_width),
        "EFFECTIVE_BW": (("row", "chan"), chan_width),
        "RESOLUTION": (("row", "chan"), chan_width),
        "NUM_CHAN": (("row",), num_chan),
        "REF_FREQUENCY": (("row",), ref_freq),
        "TOTAL_BANDWIDTH": (("row",), total_bw)
    })

    out_ddid_ds = Dataset({
        "SPECTRAL_WINDOW_ID": (("row",), spectral_window_id),
        "POLARIZATION_ID": (("row",), polarization_id),
    })

    return out_datasets, [out_spw_ds], out_ddid_ds
