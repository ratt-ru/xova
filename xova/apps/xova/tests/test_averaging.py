from itertools import product
import random

import numpy as np
import pytest


def test_bda_channelisation():
    rs = np.random.RandomState(42)

    spw_id = (0, 1, 2)
    num_chan = np.array([4, 8, 16])
    bandwidth = np.array([20, 40, 60])


    pol_id = (0, 1)

    spw_id, pol_id = (np.array(a) for a in zip(*product(spw_id, pol_id)))
    nddids = spw_id.shape[0]

    ddid = np.array([5, 1, 3, 2, 0, 4, 1, 2, 5, 3])
    row_spws = spw_id[ddid]

    row_num_chans = np.array([rs.randint(1, num_chan[s]) for s in row_spws])

    print(row_spws)
    print(row_num_chans)
