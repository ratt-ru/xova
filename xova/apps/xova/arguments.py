# -*- coding: utf-8 -*-

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import os

from loguru import logger


def create_parser():
    p = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    p.add_argument("ms", help="Input Measurement Set")
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

    return p


def log_args(args):
    logger.info("Configuration:")
    logger.info("\tAveraging '{ms}' to '{out}'", ms=args.ms, out=args.output)
    logger.info("\tAveraging {tbs} seconds together", tbs=args.time_bin_secs)
    logger.info("\tAveraging {cbs} channels together", cbs=args.chan_bin_size)

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
