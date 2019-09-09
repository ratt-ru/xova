# -*- coding: utf-8 -*-


import numpy as np


def _id(array, fill_value=0, dtype_=np.int32):
    return np.full_like(array, fill_value, dtype=dtype_)


def id_full_like(exemplar, fill_value, dtype=np.int32):
    """ full_like that handles nan chunk sizes """
    return exemplar.map_blocks(_id, fill_value=fill_value,
                               dtype_=dtype, dtype=dtype)
