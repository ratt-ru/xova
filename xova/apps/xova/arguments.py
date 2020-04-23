# -*- coding: utf-8 -*-

from argparse import (ArgumentParser,
                      ArgumentDefaultsHelpFormatter,
                      ArgumentError)
import os

from loguru import logger


def _parse_fields(field_str):
    if field_str == "":
        return []

    fields = []

    for f in (f.strip() for f in field_str.split(',')):
        try:
            fields.append(int(f))
        except ValueError:
            fields.append(f)

    return fields


def _parse_scans(scan_str):
    if scan_str == "":
        return []

    scan_numbers = []

    for s in scan_str.split(','):
        try:
            scan_numbers.append(int(s.strip()))
        except ValueError:
            raise ArgumentError("Invalid SCAN_NUMBER %s" % s)

    return scan_numbers


def _parse_channels(channel_str):
    if channel_str == "":
        return []

    channels = []

    for s in channel_str.split(','):
        rsplit = s.split("~")

        if len(rsplit) == 1:
            try:
                channels.append(int(rsplit[0].strip()))
            except ValueError:
                raise ArgumentError("Invalid Channel Number %s" % rsplit)
        elif len(rsplit) == 2:
            start, end = rsplit

            try:
                start = int(start.strip())
            except ValueError:
                raise ArgumentError(
                    "Invalid Starting Channel Number %s" % start)

            try:
                end = int(end.strip())
            except ValueError:
                raise ArgumentError("Invalid Ending Channel Number %s" % end)

            channels.append((start, end))
        else:
            raise ArgumentError("Invalid Channel Range %s" % s)

    return channels


def create_parser():
    p = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    p.add_argument("ms", help="Input Measurement Set")
    p.add_argument("-f", "--fields", type=_parse_fields, default="")
    p.add_argument("-s", "--scan-numbers", type=_parse_scans, default="")
    p.add_argument("-o", "--output",
                   help="Output Measurement Set name. "
                        "Derived from ms if not provided.")
    p.add_argument("-t", "--time-bin-secs", default=2.0, type=float,
                   help="Number of seconds to average into a single bin")
    p.add_argument("-c", "--chan-bin-size", default=16, type=int,
                   help="Number of channels to average into a single bin")
    p.add_argument("--force", action="store_true", default=False,
                   help="Force creation of OUTPUT by deleting "
                        "any existing file or directory.")
    p.add_argument("-rc", "--row-chunks", type=int, default=10000,
                   help="Number of rows averaged together "
                        "as part of a single, independent, "
                        "averaging operation.")
    p.add_argument("-grc", "--group-row-chunks", type=int, default=4,
                   help="Number of averaged row chunks to group together "
                        "when writing data to the output Measurement Set. ")
    p.add_argument("-rfr", "--respect-flag-row", action="store_true",
                   default=False,
                   help="Respects FLAG_ROW instead of overriding the column "
                        "values with a np.all(FLAG, axis=(1,2)) reduction.")
    p.add_argument("-dc", "--data-column", default="CORRECTED_DATA",
                   type=str,
                   help="Column to average. Default CORRECTED_DATA")

    return p


def log_args(args):
    logger.info("Configuration:")
    logger.info("\tAveraging '{ms}' to '{out}'", ms=args.ms, out=args.output)
    logger.info("\tAveraging {tbs} seconds together", tbs=args.time_bin_secs)
    logger.info("\tAveraging {cbs} channels together", cbs=args.chan_bin_size)
    logger.info("\tAveraging column '{col}' into 'DATA'", col=args.data_column)

    logger.info("\tApproximately {rc} rows will be averaged "
                "as independent chunks. Unique times will not be "
                "split across chunks.", rc=args.row_chunks)

    if args.group_row_chunks:
        logger.info("\tGrouping {rc} averaged row chunks "
                    "together for writing", rc=args.group_row_chunks)

    if args.force is True:
        logger.warning("\tOverwriting '{output}'", output=args.output)

    if args.respect_flag_row is True:
        logger.info("\tRespecting FLAG_ROW values")
    else:
        logger.warning("\tComputing FLAG_ROW values "
                       "via any(FLAG, axis=(1,2))")


def postprocess_args(args):
    # Create output if not specified
    if args.output is None:
        path, msname = os.path.split(args.ms.rstrip(os.sep))

        if msname[-3:].upper().endswith(".MS"):
            args.output = os.path.join(path, msname[:-3] + "_averaged.ms")
        else:
            args.output = os.path.join(path, msname + "_averaged.ms")

    return args


def parse_args(cmdline_args):
    return postprocess_args(create_parser().parse_args(cmdline_args))
