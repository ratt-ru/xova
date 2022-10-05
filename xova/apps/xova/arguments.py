# -*- coding: utf-8 -*-

from argparse import (ArgumentParser,
                      ArgumentDefaultsHelpFormatter,
                      ArgumentError)
import os
import sys

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
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    sp = parser.add_subparsers(help='command', dest='command')
    tc_parser = sp.add_parser("timechannel")
    bda_parser = sp.add_parser("bda")

    # Set up time channel and bda parsers
    for p in (tc_parser, bda_parser):
        p.add_argument("ms", help="Input Measurement Set")
        p.add_argument("-f", "--fields", type=_parse_fields, default="")
        p.add_argument("-s", "--scan-numbers", type=_parse_scans, default="")
        p.add_argument("-o", "--output",
                       help="Output Measurement Set name. "
                            "Derived from ms if not provided.")
        p.add_argument("--force", action="store_true", default=False,
                       help="Force creation of OUTPUT by deleting "
                            "any existing file or directory.")
        p.add_argument("-rc", "--row-chunks", type=int, default=10000,
                       help="Number of rows averaged together "
                            "as part of a single, independent, "
                            "averaging operation.")
        p.add_argument("-grc", "--group-row-chunks", type=int, default=4,
                       help="Number of averaged row chunks to "
                            "group together when writing data "
                            "to the output Measurement Set.")
        p.add_argument("-rfr", "--respect-flag-row", action="store_true",
                       default=False,
                       help="Respects FLAG_ROW instead of overriding the "
                            "column values with a np.all(FLAG, axis=(1,2)) "
                            "reduction.")
        p.add_argument("-dc", "--data-column", default=["CORRECTED_DATA:DATA"],
                       type=str, nargs="+",
                       help="Column(s) to average. Default CORRECTED_DATA:DATA")

        p.add_argument("--include-auto-correlations", action="store_true",
                       default=False,
                       help="Auto-correlations are removed from the output "
                            "by default. Set this to include them.")
        p.add_argument("--average-uvw-coordinates", action="store_true",
                       default=False,
                       help="Skips recomputation of uvw coordinates at new time "
                            "centroids. This could be faster on small arrays "
                            "but is highly discouraged since your uvw coordinates "
                            "will be approximated by simple weighted Euclidian mean")
        p.add_argument("--boring", action="store_true",
                       default=False,
                       help="Disables interactive tty printing, e.g. progress "
                            "bar.")

    # Time channel specific args
    tc_parser.add_argument("-t", "--time-bin-secs", default=2.0, type=float,
                           help="Number of seconds to "
                                "average into a single bin")
    tc_parser.add_argument("-c", "--chan-bin-size", default=16, type=int,
                           help="Number of channels to "
                                "average into a single bin")

    # BDA Specific args
    bda_parser.add_argument("-t", "--time-bin-secs", default=None, type=float,
                            help="Number of seconds to "
                                 "average into a single bin")
    bda_parser.add_argument("-d", "--decorrelation", default=.99, type=float,
                            help="Acceptable decorrrelation factor")
    bda_parser.add_argument("-fov", "--max-fov", default=15.0, type=float,
                            help="Maximum Field of View (radius) in degrees")
    bda_parser.add_argument("-mc", "--min-nchan", default=1, type=int,
                            help="Minimum number of channels in output. "
                                 "This will be rounded up to the nearest "
                                 "integer factorisation of the "
                                 "number of input channels")

    # Table Conformance Checking
    check_parser = sp.add_parser("check")

    check_parser.add_argument("ms", help="Input Measurement Set")
    check_parser.add_argument("-r", "--row-chunks", type=int, default=10000)

    return parser


def log_args(args):
    logger.info("Configuration:")

    if args.command == "timechannel":
        logger.info("Standard Time and Channel Averaging")
    elif args.command == "bda":
        logger.info("Baseline-Dependant Time and Channel Averaging")
    elif args.command == "check":
        logger.info(f"Checking MS conformance of {args.ms}")
        return

    logger.info(f"\tAveraging '{args.ms}' "
                f"to '{args.output}'")

    if args.command == "timechannel":
        logger.info(f"\tAveraging {args.time_bin_secs} seconds together")
        logger.info(f"\tAveraging {args.chan_bin_size} "
                    f"channels together")
    elif args.command == "bda":
        logger.info(f"\tAcceptable decorrelation factor "
                    f"{args.decorrelation}")

        if args.time_bin_secs is not None:
            logger.info(f"\tAveraging {args.time_bin_secs} seconds together")

        logger.info(f"\tMaximum Field of View {args.max_fov} degrees")
        logger.info(f"\tMinimum number of channels in "
                    f"output {args.min_nchan}. "
                    f"This will be rounded up to the nearest "
                    f"integer factorisation of the "
                    f"number of input channels")

    for from_column, to_column in args.data_column.items():
        logger.info(f"\tAveraging {from_column} into {to_column}")

    logger.info(f"\tApproximately {args.row_chunks} rows will be averaged "
                f"as independent chunks. Unique times will not be "
                f"split across chunks.")

    if args.group_row_chunks:
        logger.info(f"\tGrouping {args.group_row_chunks} "
                    f"averaged row chunks together for writing")

    if args.include_auto_correlations:
        logger.info("\tIncluding Auto-Correlations")
    else:
        logger.warning("\tDiscarding Auto-Correlations")

    if args.force is True:
        logger.warning(f"\tOverwriting '{args.output}'")

    if args.respect_flag_row is True:
        logger.info("\tRespecting FLAG_ROW values")
    else:
        logger.warning("\tOverriding FLAG_ROW values "
                       "with any(FLAG, axis=(1,2))")


def postprocess_args(args):
    if args.command == "check":
        return args

    args.taql_where = ("" if args.include_auto_correlations
                       else "ANTENNA1 != ANTENNA2")

    # Create output if not specified
    if args.output is None:
        path, msname = os.path.split(args.ms.rstrip(os.sep))

        if msname[-3:].upper().endswith(".MS"):
            args.output = os.path.join(path, msname[:-3] + "_averaged.ms")
        else:
            args.output = os.path.join(path, msname + "_averaged.ms")

    data_column_map = {}

    for column in args.data_column:
        csplit = column.split(":")

        if len(csplit) == 2:
            from_column, to_column = csplit
        elif len(csplit) == 1:
            from_column = to_column = csplit[0]
        else:
            raise ValueError(f"Invalid data columns: {args.data_column}")

        data_column_map[from_column] = to_column

    args.data_column = data_column_map

    return args


def parse_args(cmdline_args):
    parser = create_parser()
    args = parser.parse_args(cmdline_args)

    if not args.command:
        parser.print_help()
        sys.exit(0)

    return postprocess_args(args)
