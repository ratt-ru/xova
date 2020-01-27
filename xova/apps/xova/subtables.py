# -*- coding: utf-8 -*-

import shutil

from loguru import logger
import pyrap.tables as pt


def copy_subtables(ms, output, subtables):
    """
    Faster to just shutil.copytree the tables across
    """
    assert "SPECTRAL_WINDOW" not in subtables

    keywords = {}

    for subtable in sorted(subtables):
        in_tab = "/".join((ms, subtable))
        out_tab = "/".join((output, subtable))
        logger.info("Copying {in_tab} to {out_tab}",
                    in_tab=in_tab, out_tab=out_tab)

        # Remove destination folder if it exists
        shutil.rmtree(out_tab, ignore_errors=True)
        # Copy the tree
        shutil.copytree(in_tab, out_tab)
        keywords[subtable] = "Table: " + out_tab

    # Need to open with manual locking to interact with dask-ms TableProxies
    with pt.table(output, ack=False, readonly=False, lockoptions='user') as T:
        T.lock(True)  # Acquire write lock

        try:
            # Link the sub-tables to the output MS
            T.putkeywords(keywords)
        finally:
            T.unlock()
