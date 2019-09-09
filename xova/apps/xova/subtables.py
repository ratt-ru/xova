# -*- coding: utf-8 -*-

import os
import shutil

from loguru import logger
import pyrap.tables as pt


def copy_subtables(ms, output, subtables):
    """
    Faster to just shutil.copytree the tables across
    """
    with pt.table(output, ack=False, readonly=False, lockoptions='user') as T:
        T.lock(True)

        try:
            for subtable in subtables:
                in_tab = "/".join((ms, subtable))
                out_tab = "/".join((output, subtable))
                logger.info("Copying {in_tab} to {out_tab}",
                            in_tab=in_tab, out_tab=out_tab)

                # Remove destination folder if it exists
                shutil.rmtree(out_tab, ignore_errors=True)
                # Ignore the lock file
                ignore = shutil.ignore_patterns("table.lock")
                # Copy the tree
                shutil.copytree(in_tab, out_tab, ignore=ignore)

                # Link the sub-table to the output MS
                T.putkeyword(subtable, "Table: " + out_tab)
        finally:
            T.unlock()
