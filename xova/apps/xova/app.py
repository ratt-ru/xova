# -*- coding: utf-8 -*-

import traceback
import warnings
import sys
from contextlib import ExitStack
import os
import shutil
import dask
from daskms import xds_from_ms, xds_from_table, xds_to_table
from loguru import logger

from xova import __version__ as xova_version
import xova.apps.xova.logger_init  # noqa
from xova.apps.xova.arguments import parse_args, log_args
from xova.apps.xova.averaging import (average_main,
                                      bda_average_main,
                                      average_spw,
                                      bda_average_spw)
from xova.apps.xova.check import check_ms
from xova.apps.xova.chunking import dataset_chunks
from xova.apps.xova.subtables import copy_subtables
from xova.apps.xova.fixvis import fixms


def warn_with_traceback(message, category, filename, lineno, file=None, line=None):
    log = file if hasattr(file, 'write') else sys.stderr
    traceback.print_stack(file=log)
    log.write(warnings.formatwarning(message, category, filename, lineno, line))


warnings.showwarning = warn_with_traceback


try:
    import bokeh  # noqa
except ImportError:
    can_profile = False
else:
    can_profile = True


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
        logger.info("xova version {v}", v=xova_version)
        logger.info("xova {args}", args=" ".join(self.cmdline_args))

        self.args = args = parse_args(self.cmdline_args)
        log_args(args)

        if args.command == "check":
            check_ms(args)
            logger.info("{ms} is conformant".format(ms=args.ms))
            return

        self._maybe_remove_output_ms(args)

        row_chunks, time_chunks, interval_chunks = self._derive_row_chunking(
            args)

        (main_ds, spw_ds,
         ddid_ds, field_ds,
         subtables) = self._input_datasets(args, row_chunks)

        # Set up Main MS data averaging
        if args.command == "timechannel":
            output_ds = average_main(main_ds,
                                     field_ds,
                                     args.time_bin_secs,
                                     args.chan_bin_size,
                                     args.fields,
                                     args.scan_numbers,
                                     args.group_row_chunks,
                                     args.respect_flag_row,
                                     viscolumn=args.data_column)

            spw_ds = average_spw(spw_ds, args.chan_bin_size)
        elif args.command == "bda":
            output_ds = bda_average_main(main_ds,
                                         field_ds,
                                         ddid_ds,
                                         spw_ds,
                                         args)

            output_ds, spw_ds, out_ddid_ds = bda_average_spw(output_ds,
                                                             ddid_ds,
                                                             spw_ds)
        else:
            raise ValueError("Invalid command %s" % args.command)

        main_writes = xds_to_table(output_ds, args.output, "ALL",
                                   descriptor="ms(False)")

        spw_table = "::".join((args.output, "SPECTRAL_WINDOW"))
        spw_writes = xds_to_table(spw_ds, spw_table, "ALL")

        if args.command == "bda":
            ddid_table = "::".join((args.output, "DATA_DESCRIPTION"))
            ddid_writes = xds_to_table(out_ddid_ds, ddid_table, "ALL")
            subtables.discard("DATA_DESCRIPTION")
        else:
            ddid_writes = None

        copy_subtables(args.ms, args.output, subtables)

        self._execute_graph(main_writes, spw_writes, ddid_writes)
        if not args.average_uvw_coordinates:
            fixms(args.output)
        else:
            logger.warning("Applying approximation to uvw coordinates as you requested - "
                           "the spatial frequencies of your long baseline data may be "
                           "serverely affected!")

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

            if sys.stdout.isatty() and not self.args.boring:
                from dask.diagnostics import ProgressBar
                stack.enter_context(ProgressBar())
            dask.compute(*writes, scheduler='single-threaded')
            logger.info("Averaging Complete")

        if can_profile:
            visualize(profilers)

    def _maybe_remove_output_ms(self, args):
        # Check for existence of output
        if os.path.exists(args.output):
            if args.force is True:
                shutil.rmtree(args.output)
            else:
                raise ValueError("'{out}' exists. Use --force to overwrite"
                                 .format(out=args.output))

    def _derive_row_chunking(self, args):
        datasets = xds_from_ms(args.ms, group_cols=GROUP_COLS,
                               columns=["TIME", "INTERVAL", "UVW"],
                               chunks={'row': args.row_chunks},
                               taql_where=args.taql_where)

        return dataset_chunks(datasets, args.time_bin_secs,
                              args.row_chunks, bda=args.command)

    def _input_datasets(self, args, row_chunks):
        # Set up row chunks
        chunks = [{'row': rc} for rc in row_chunks]

        main_ds, tabkw = xds_from_ms(args.ms,
                                     group_cols=GROUP_COLS,
                                     table_keywords=True,
                                     chunks=chunks,
                                     taql_where=args.taql_where)

        # Figure out non SPW + SORTED sub-tables to just copy
        subtables = {k for k, v in tabkw.items() if
                     k not in ("SPECTRAL_WINDOW", "SORTED_TABLE") and
                     isinstance(v, str) and v.startswith("Table: ")}

        spw_ds = xds_from_table("::".join((args.ms, "SPECTRAL_WINDOW")),
                                group_cols="__row__")

        field_ds = xds_from_table("::".join((args.ms, "FIELD")),
                                  group_cols="__row__")

        ddid_ds = xds_from_table("::".join((args.ms, "DATA_DESCRIPTION")))
        assert len(ddid_ds) == 1
        ddid_ds = ddid_ds[0].compute()

        return main_ds, spw_ds, ddid_ds, field_ds, subtables
