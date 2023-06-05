import argparse
import configparser
import datetime
import glob
import logging
import multiprocessing
import os
import sys
import time
from functools import partial
import ray

from histoqc._pipeline import load_pipeline
from histoqc.config import read_config_template
from histoqc.data import managed_pkg_data

@managed_pkg_data
def main(argv=None):
    """main entry point for histoqc pipelines"""
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(prog="histoqc", description='Run HistoQC main quality control pipeline for digital pathology images')
    parser.add_argument('input_pattern',
                        help="input filename pattern (try: *.svs or target_path/*.svs ),"
                             " or tsv file containing list of files to analyze",
                        nargs="+")
    parser.add_argument('-o', '--outdir',
                        help="outputdir, default ./histoqc_output_YYMMDD-hhmmss",
                        default=f"./histoqc_output_{time.strftime('%Y%m%d-%H%M%S')}",
                        type=str)
    parser.add_argument('-p', '--basepath',
                        help="base path to add to file names,"
                             " helps when producing data using existing output file as input",
                        default="",
                        type=str)
    parser.add_argument('-c', '--config',
                        help="config file to use, either by name supplied by histoqc.config (e.g., v2.1) or filename",
                        type=str)
    parser.add_argument('-f', '--force',
                        help="force overwriting of existing files",
                        action="store_true")
    parser.add_argument('-b', '--batch',
                        help="break results file into subsets of this size",
                        type=int,
                        default=None)
    parser.add_argument('-n', '--nprocesses',
                        help="number of processes to launch",
                        type=int,
                        default=1)
    parser.add_argument('--symlink', metavar="TARGET_DIR",
                        help="create symlink to outdir in TARGET_DIR",
                        default=None)
    args = parser.parse_args(argv)

    config = configparser.ConfigParser()

    if not args.config:
        print(f"Configuration file not set (--config), using default")
        config.read_string(read_config_template('default'))
    elif os.path.exists(args.config):
        config.read(args.config) #Will read the config file
    else:
        print(f"Configuration file {args.config} assuming to be a template...checking.")
        config.read_string(read_config_template(args.config))

    process_queue = load_pipeline(config)
    print(process_queue)

    


if __name__ == "__main__":
    sys.exit(main())