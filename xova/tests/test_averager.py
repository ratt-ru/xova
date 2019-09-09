import numpy as np
from africanus.averaging import time_and_channel


def test_stuff():
    time = np.linspace(0.1, 1.0, 10)
    interval = np.repeat(1.0, 10)
    ant1, ant2 = (a.astype(np.int32) for a in np.triu_indices(5, 1))
    chan_freq = np.linspace(.856e9, 2*.856e9, 16)

    res = time_and_channel(time, interval, ant1, ant2, chan_freq=chan_freq,
                           chan_bin_size=4)

    print(res)
