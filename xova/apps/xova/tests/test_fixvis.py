# -*- coding: utf-8 -*-
# flake8: noqa

from xova.apps.xova.fixvis import (synthesize_uvw,
                                   dense2sparse_uvw)
import numpy as np
import time

DEBUG = False

if DEBUG:
    from matplotlib import pyplot as plt

vla_d = np.asarray(
        [[-1.60171e+06, -5.04201e+06, 3.5546e+06],
         [-1.60115e+06, -5.042e+06, 3.55486e+06],
         [-1.60072e+06, -5.04227e+06, 3.55467e+06],
         [-1.60119e+06, -5.042e+06, 3.55484e+06],
         [-1.60161e+06, -5.042e+06, 3.55465e+06],
         [-1.60116e+06, -5.04183e+06, 3.5551e+06],
         [-1.60101e+06, -5.04209e+06, 3.5548e+06],
         [-1.60119e+06, -5.04198e+06, 3.55488e+06],
         [-1.60095e+06, -5.04213e+06, 3.55477e+06],
         [-1.60118e+06, -5.04193e+06, 3.55495e+06],
         [-1.60107e+06, -5.04205e+06, 3.55482e+06],
         [-1.6008e+06, -5.04222e+06, 3.55471e+06],
         [-1.60116e+06, -5.04178e+06, 3.55516e+06],
         [-1.60145e+06, -5.04199e+06, 3.55474e+06],
         [-1.60123e+06, -5.04198e+06, 3.55486e+06],
         [-1.60153e+06, -5.042e+06, 3.5547e+06],
         [-1.60114e+06, -5.04168e+06, 3.55532e+06],
         [-1.60132e+06, -5.04199e+06, 3.55481e+06],
         [-1.60117e+06, -5.04187e+06, 3.55504e+06],
         [-1.60119e+06, -5.04202e+06, 3.55481e+06],
         [-1.60117e+06, -5.0419e+06, 3.55499e+06],
         [-1.60088e+06, -5.04217e+06, 3.55474e+06],
         [-1.60138e+06, -5.04199e+06, 3.55478e+06],
         [-1.60118e+06, -5.04195e+06, 3.55492e+06],
         [-1.60127e+06, -5.04198e+06, 3.55483e+06],
         [-1.60111e+06, -5.04202e+06, 3.55484e+06],
         [-1.60115e+06, -5.04173e+06, 3.55524e+06]])


def test_synthesize_uvw():
    na = vla_d.shape[0]
    nbl = na * (na - 1) // 2 + na
    ntime = 50
    padded_a1 = np.empty((nbl), dtype=np.int32)
    padded_a2 = np.empty((nbl), dtype=np.int32)
    antindies = np.stack(np.triu_indices(na, 0),
                         axis=1)
    for bl in range(nbl):
        blants = antindies[bl]
        padded_a1[bl] = blants[0]
        padded_a2[bl] = blants[1]
    timearr = np.linspace(4789242432.0, 4789268088.5, ntime).repeat(nbl)
    padded_a1 = np.tile(padded_a1, ntime)
    padded_a2 = np.tile(padded_a2, ntime)
    uvw = synthesize_uvw(vla_d, timearr, padded_a1, padded_a2, 
                         np.deg2rad([[60, 30]]))
    if DEBUG:
        plt.figure(figsize=(7, 7))
        plt.plot(uvw["UVW"][:, 0], uvw["UVW"][:, 1], ".r", markersize=0.5)
        plt.grid(True)
        plt.xlabel("u (m)")
        plt.ylabel("v (m)")
        plt.show(False)
        time.sleep(3) 

def test_uvw_sparse():
    na = vla_d.shape[0]
    nbl = na * (na - 1) // 2 + na
    ntime = 100
    padded_a1 = np.empty((nbl), dtype=np.int32)
    padded_a2 = np.empty((nbl), dtype=np.int32)
    antindies = np.stack(np.triu_indices(na, 0),
                         axis=1)
    for bl in range(nbl):
        blants = antindies[bl]
        padded_a1[bl] = blants[0]
        padded_a2[bl] = blants[1]
    timearr = np.linspace(4789242432.0, 4789268088.5, ntime).repeat(nbl)
    padded_a1 = np.tile(padded_a1, ntime)
    padded_a2 = np.tile(padded_a2, ntime)
    ddid = np.zeros(padded_a1.size)
    nrow = padded_a1.size
    uvw_dense = synthesize_uvw(vla_d, timearr, padded_a1,
                               padded_a2, np.deg2rad([[60, 30]]))
    
    # remove some rows
    np.random.seed(42)
    sel = sorted(set(np.arange(nrow)) - 
                 set(np.random.randint(low=0,
                                       high=nrow-1,
                                       size=int(0.20 * nrow))))
    reindx = np.arange(nrow)
    np.random.shuffle(reindx) # in place shuffle
    miss_timearr = timearr[reindx][sel]
    miss_a1 = padded_a1[reindx][sel]
    miss_a2 = padded_a2[reindx][sel]
    miss_ddid = ddid[reindx][sel]
    uvw_miss = synthesize_uvw(vla_d, miss_timearr, miss_a1,
                              miss_a2, np.deg2rad([[60, 30]]))
    # even with missing rows the synthesis routine should
    # give the same UV coordinates as we use a per antenna
    # delay decomposition
    nrow = uvw_dense["UVW"].shape[0]
    for c in ["UVW", "TIME_CENTROID", "ANTENNA1", "ANTENNA2"]:
        assert np.all(uvw_dense[c] == uvw_miss[c])
        assert uvw_dense[c].shape[0] == nrow
        assert uvw_miss[c].shape[0] == nrow
        
    uvw_sparse = dense2sparse_uvw(a1=miss_a1, a2=miss_a2, time=miss_timearr,
                                  ddid=miss_ddid, padded_uvw=uvw_dense["UVW"])
    
    ala = np.logical_and
    alo = np.logical_or
    for rin in range(nrow):
        a1in = uvw_dense["ANTENNA1"][rin]
        a2in = uvw_dense["ANTENNA2"][rin]
        tin = uvw_dense["TIME_CENTROID"][rin]
        oblsel = alo(ala(miss_a1 == a1in, 
                         miss_a2 == a2in),
                     ala(miss_a2 == a1in, 
                         miss_a1 == a2in))
        otsel = miss_timearr == tin
        osel = ala(oblsel, otsel)
        # only 1 ddid here so no further selection
        assert np.sum(osel) <= 1
        if np.sum(osel) == 1: # may not have an output row in the selection
            assert np.all(uvw_sparse[osel] == uvw_dense["UVW"][rin])

if __name__ == "__main__":
    test_synthesize_uvw()
    test_uvw_sparse()