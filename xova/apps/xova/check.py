# -*- coding: utf-8 -*-

import numpy as np
import pyrap.tables as pt


class TableConformanceError(Exception):
    pass


def _check_column_shape(column, column_name, num_var_name, shape):
    if column.shape == shape:
        return

    raise TableConformanceError(
        "Shape of {column_name} doesn't match the "
        "associated {num_var_name} value"
        .format(column_name=column_name,
                num_var_name=num_var_name))


def check_ms(args):
    """ Entrypoint, call with arguments """

    Q = pt.taql("SELECT *, SHAPE(DATA) AS DATA_SHAPE FROM {ms}"
                .format(ms=args.ms))
    D = pt.table("::".join((args.ms, "DATA_DESCRIPTION")), ack=False)
    S = pt.table("::".join((args.ms, "SPECTRAL_WINDOW")), ack=False)
    P = pt.table("::".join((args.ms, "POLARIZATION")), ack=False)

    spw_id = D.getcol("SPECTRAL_WINDOW_ID")
    pol_id = D.getcol("POLARIZATION_ID")
    num_chan = S.getcol("NUM_CHAN")
    num_corr = P.getcol("NUM_CORR")

    # Check channel and corrlation conformance for
    # visibility data
    for r in range(0, Q.nrows(), args.row_chunks):
        nrow = min(args.row_chunks, Q.nrows() - r)
        ddid = Q.getcol("DATA_DESC_ID", startrow=r, nrow=nrow)
        data_shape = Q.getcol("DATA_SHAPE", startrow=r, nrow=nrow)

        nchan = num_chan[spw_id[ddid]]
        ncorr = num_corr[pol_id[ddid]]

        shape = np.stack([nchan, ncorr], axis=1)
        if not np.array_equal(data_shape, shape):
            raise TableConformanceError(
                "DATA shapes don't match "
                "SPECTRAL_WINDOW.NUM_CHAN and "
                "POLARIZATION.NUM_CORR mapped via "
                "DATA_DESC_ID")

    for spw, nchan in enumerate(num_chan):
        chan_width = S.getcol("CHAN_WIDTH", startrow=spw, nrow=1)
        chan_freq = S.getcol("CHAN_FREQ", startrow=spw, nrow=1)
        effective_bw = S.getcol("EFFECTIVE_BW", startrow=spw, nrow=1)
        resolution = S.getcol("RESOLUTION", startrow=spw, nrow=1)

        _check_column_shape(chan_width, "CHAN_WIDTH",
                            "NUM_CHAN", (1, nchan))
        _check_column_shape(chan_freq, "CHAN_FREQ",
                            "NUM_CHAN", (1, nchan))
        _check_column_shape(effective_bw, "EFFECTIVE_BW",
                            "NUM_CHAN", (1, nchan))
        _check_column_shape(resolution, "RESOLUTION",
                            "NUM_CHAN", (1, nchan))

    for pol, ncorr in enumerate(num_corr):
        corr_type = P.getcol("CORR_TYPE", startrow=pol, nrow=1)
        corr_product = P.getcol("CORR_PRODUCT", startrow=pol, nrow=1)

        _check_column_shape(corr_type, "CORR_TYPE",
                            "NUM_CORR", (1, ncorr))
        _check_column_shape(corr_product, "CORR_PRODUCT",
                            "NUM_CORR", (1, ncorr, 2))
