# -*- coding: utf-8 -*-

from contextlib import ExitStack
import os
import shutil
import sys

try:
    import bokeh  # noqa
except ImportError:
    can_profile = False
else:
    can_profile = True

import dask
from daskms import xds_from_ms, xds_from_table, xds_to_table
from loguru import logger

import xova.apps.xova.logger_init  # noqa
from xova.apps.xova.arguments import parse_args, log_args
from xova.apps.xova.averaging import average_main, average_spw
from xova.apps.xova.chunking import dataset_chunks
from xova.apps.xova.subtables import copy_subtables


GROUP_COLS = ["FIELD_ID", "DATA_DESC_ID", "SCAN_NUMBER"]


def main():
    """ Entrypoint, call with arguments """
    Application(sys.argv[1:]).execute()


class Application(object):
    """
    Class responsible for implementing the Averaging Operation.
    """

    def __init__(self, cmdline_args):
        self.cmdline_args = cmdline_args

    def execute(self):
        """ Execute the application """
        logger.info("xova {args}", args=" ".join(self.cmdline_args))

        self.args = args = parse_args(self.cmdline_args)
        log_args(args)
        self._create_output_ms(args)

        row_chunks, time_chunks = self._derive_row_chunking(args)
        (main_ds, spw_ds,
         ddid_ds, subtables) = self._input_datasets(args, row_chunks)

        # Set up Main MS data averaging
        main_ds = average_main(main_ds,
                               args.time_bin_secs,
                               args.chan_bin_size,
                               args.group_row_chunks,
                               args.respect_flag_row)

        main_writes = xds_to_table(main_ds, args.output, "ALL")

        # Set up SPW data averaging
        spw_ds = average_spw(spw_ds, args.chan_bin_size)
        spw_table = "::".join((args.output, "SPECTRAL_WINDOW"))
        spw_writes = xds_to_table(spw_ds, spw_table, "ALL")

        copy_subtables(args.ms, args.output, subtables)

        self._execute_graph(main_writes, spw_writes)

    def _execute_graph(self, *writes):
        # Set up Profilers and Progress Bars
        with ExitStack() as stack:
            profilers = []

            if can_profile:
                from dask.diagnostics import (Profiler, CacheProfiler,
                                              ResourceProfiler, visualize)

                profilers.append(stack.enter_context(Profiler()))
                profilers.append(stack.enter_context(CacheProfiler()))
                profilers.append(stack.enter_context(ResourceProfiler()))

            if sys.stdout.isatty():
                from dask.diagnostics import ProgressBar
                stack.enter_context(ProgressBar())

            dask.compute(*writes)
            logger.info("Averaging Complete")

        if can_profile:
            visualize(profilers)

    def _create_output_ms(self, args):
        # Check for existence of output
        if os.path.exists(args.output):
            if args.force is True:
                shutil.rmtree(args.output)
            else:
                raise ValueError("'{out}' exists. Use --force to overwrite"
                                 .format(out=args.output))

    def _derive_row_chunking(self, args):
        datasets = xds_from_ms(args.ms, group_cols=GROUP_COLS,
                               columns=["TIME", "INTERVAL"],
                               chunks={'row': args.row_chunks})

        return dataset_chunks(datasets, args.time_bin_secs,
                              args.row_chunks)

    def _input_datasets(self, args, row_chunks):
        # Set up row chunks
        chunks = [{'row': rc} for rc in row_chunks]

        main_ds, tabkw, colkw = xds_from_ms(args.ms,
                                            group_cols=GROUP_COLS,
                                            table_keywords=True,
                                            column_keywords=True,
                                            chunks=chunks)

        # Figure out non SPW + SORTED sub-tables to just copy
        subtables = {k for k, v in tabkw.items() if
                     k not in ("SPECTRAL_WINDOW", "SORTED_TABLE") and
                     isinstance(v, str) and v.startswith("Table: ")}

        spw_ds = xds_from_table("::".join((args.ms, "SPECTRAL_WINDOW")),
                                group_cols="__row__")

        ddid_ds = xds_from_table("::".join((args.ms, "DATA_DESCRIPTION")))
        assert len(ddid_ds) == 1
        ddid_ds = dask.compute(ddid_ds)[0]

        return main_ds, spw_ds, ddid_ds, subtables
