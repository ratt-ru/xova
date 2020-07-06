from itertools import product

import dask.array as da
from daskms.dataset import Dataset
import numpy as np

from xova.apps.xova.averaging import bda_average_spw


def test_bda_channelisation():
    rs = np.random.RandomState(42)

    spw_id = (0, 1, 2)
    pol_id = (0, 1)

    nchan = np.array([4, 8, 16])
    bandwidth = np.array([20.0, 40.0, 60.0])
    ref_freq = np.array([100.0, 200.0, 500.0])
    chan_freq = [np.linspace(rf - bw / 2, rf + bw / 2, nc)[None, :]
                 for rf, bw, nc
                 in zip(ref_freq, bandwidth, nchan)]
    chan_width = [np.full(nc, bw / nc)[None, :] for nc, bw
                  in zip(nchan, bandwidth)]

    spw_id, pol_id = (np.array(a) for a in zip(*product(spw_id, pol_id)))

    ddid = np.array([5, 1, 3, 2, 0, 4, 1, 2, 5, 3])
    time = np.linspace(5.03373334e+09, 5.03373362e+09, ddid.shape[0])
    row_chunks = (4, 3, 1, 2)
    row_spws = spw_id[ddid]
    num_chan = nchan[row_spws]

    row_num_chans = np.array([rs.randint(1, num_chan[s]) for s in row_spws])

    da_ddid = da.from_array(ddid, chunks=(row_chunks,))
    da_num_chan = da.from_array(row_num_chans, chunks=(row_chunks),)
    da_time = da.from_array(time, chunks=(row_chunks,))

    out_ds = [Dataset({
                "DATA_DESC_ID": (("row",), da_ddid),
                "TIME": (("row",), da_time),
                "NUM_CHAN": (("row",), da_num_chan),
                "DECORR_CHAN_WIDTH": (("row",), da.zeros_like(time))
            })]

    ddid_ds = Dataset({
        "SPECTRAL_WINDOW_ID": (("row",), spw_id),
        "POLARIZATION_ID": (("row",), pol_id),
    })

    spw_ds = [
        Dataset({
            "REF_FREQUENCY": (("row",), da.from_array([rf],
                                                      chunks=1)),
            "NUM_CHAN": (("row",), da.from_array([nc],
                                                 chunks=1)),
            "CHAN_FREQ": (("row",), da.from_array(chan_freq[spw],
                                                  chunks=(1, nc))),
            "CHAN_WIDTH": (("row",), da.from_array(chan_width[spw],
                                                   chunks=(1, nc))),
            "RESOLUTION": (("row",), da.from_array(chan_width[spw],
                                                   chunks=(1, nc))),
            "EFFECTIVE_BW": (("row",), da.from_array(chan_width[spw],
                                                     chunks=(1, nc))),
            "TOTAL_BANDWIDTH": (("row"), da.from_array([bw],
                                                       chunks=1))
        })

        for spw, (rf, bw, nc) in enumerate(zip(ref_freq, bandwidth, nchan))
    ]

    out_ds, spw_ds, ddid_ds = bda_average_spw(out_ds, ddid_ds, spw_ds)
    out_ds, spw_ds, ddid_ds = da.compute(out_ds, spw_ds, ddid_ds)
