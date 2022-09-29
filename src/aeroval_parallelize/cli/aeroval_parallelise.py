#!/usr/bin/env python3
"""
command line interface for parallelisation for aeroval processing

- create several aeroval config files from one input config
  (per model and per obs network for now)
- submit these configs to the GridEngine queue

"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from aeroval_parallelize.tools import (
    AEROVAL_HEATMAP_FILES_MASK,
    AEROVAL_HEATMAP_TS_FILES_MASK,
    CONDA_ENV,
    DEFAULT_CFG_VAR,
    JSON_RUNSCRIPT,
    QSUB_QUEUE_NAME,
    QSUB_USER,
    TMP_DIR,
    adjust_heatmapfile,
    adjust_hm_ts_file,
    adjust_menujson,
    combine_output,
    prep_files,
    read_config_var,
    run_queue,
)

def main():
    """main program"""

    # define some terminal colors to be used in the help
    colors = {
        "BOLD": "\033[1m",
        "UNDERLINE": "\033[4m",
        "END": "\033[0m",
        "PURPLE": "\033[95m",
        "CYAN": "\033[96m",
        "DARKCYAN": "\033[36m",
        "BLUE": "\033[94m",
        "GREEN": "\033[92m",
        "YELLOW": "\033[93m",
        "RED": "\033[91m",
    }
    script_name = Path(sys.argv[0]).name
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="command line interface to aeroval parallelisation.",
        epilog=f"""{colors['BOLD']}Example usages:{colors['END']}

{colors['UNDERLINE']}run script on qsub host and do not submit jobs to queue:{colors['END']}
    {script_name} --noqsub -l <cfg-file>

{colors['UNDERLINE']}run script on workstation, set directory of aeroval files and submit to queue via qsub host:{colors['END']}
   {script_name}  --remotetempdir <directory for aeroval files> <cfg-file>
   
   Note that the directory for aeroval files needs to be on a common file system for all cluster machines.
   
{colors['UNDERLINE']}set data directories and submit to queue:{colors['END']}
    {script_name} --json_basedir /tmp/data --coldata_basedir /tmp/coldata --io_aux_file /tmp/gridded_io_aux.py <cfg-file>

{colors['UNDERLINE']}assemble aeroval data after a parallel run has been finished: (runs always on the local machine){colors['END']}
    {script_name} -c -o <output directory> <input directories>
    {script_name} -c -o ${{HOME}}/tmp ${{HOME}}/tmpt39n2gp_*

{colors['UNDERLINE']}adjust all variable and model orders to the one given in a aeroval config file:{colors['END']}
    {script_name} --adjustall <aeroval-cfg-file> <path to menu.json>
    {script_name} --adjustall  /tmp/config/cfg_cams2-82_IFS_beta.py /tmp/data/testmerge_all/IFS-beta/menu.json

    
""",
    )
    parser.add_argument(
        "files",
        help="file(s) to read, directories to combine (if -c switch is used)",
        nargs="+",
    )
    parser.add_argument("-v", "--verbose", help="switch on verbosity", action="store_true")

    parser.add_argument(
        "-e",
        "--env",
        help=f"conda env used to run the aeroval analysis; defaults to {CONDA_ENV}",
    )
    parser.add_argument(
        "--queue",
        help=f"queue name to submit the jobs to; defaults to {QSUB_QUEUE_NAME}",
    )
    parser.add_argument("--queue-user", help=f"queue user; defaults to {QSUB_USER}")
    parser.add_argument(
        "--noqsub",
        help="do not submit to queue (all files created and copied, but no submission)",
        action="store_true",
    )
    # parser.add_argument("--noobsnetparallelisation",
    #                     help="don't pa",
    #                     action="store_true")
    parser.add_argument(
        "--jsonrunscript",
        help=f"script to run json config files; defaults to {JSON_RUNSCRIPT}",
        default=JSON_RUNSCRIPT,
    )
    parser.add_argument(
        "--cfgvar",
        help=f"variable that holds the aeroval config in the file(s) provided. Defaults to {DEFAULT_CFG_VAR}",
        default=DEFAULT_CFG_VAR,
    )
    parser.add_argument(
        "--tempdir",
        help=f"directory for temporary files; defaults to {TMP_DIR}",
        default=TMP_DIR,
    )
    parser.add_argument(
        "--remotetempdir",
        help=f"directory for temporary files on qsub node; defaults to {TMP_DIR}",
        default=TMP_DIR,
    )
    parser.add_argument(
        "--json_basedir",
        help="set json_basedir in the config manually",
    )
    parser.add_argument(
        "--coldata_basedir",
        help="set coldata_basedir in the configuration manually",
    )
    parser.add_argument(
        "--io_aux_file",
        help="set io_aux_file in the configuration file manually",
    )

    parser.add_argument(
        "-l",
        "--localhost",
        help="start queue submission on localhost",
        action="store_true",
    )

    group_assembly = parser.add_argument_group(
        "data assembly:", "options for assembly of parallisations output"
    )
    group_assembly.add_argument("-o", "--outdir", help="output directory for experiment assembly")
    group_assembly.add_argument(
        "-c",
        "--combinedirs",
        help="combine the output of a parallel runs",
        action="store_true",
    )
    group_menujson = parser.add_argument_group(
        "adjust variable and model order",
        "options to change existing order of variables and models",
    )
    group_menujson.add_argument(
        "-a",
        "--adjustall",
        help=" <aeroval cfgfile> <path to menu.json>; adjust order of all models/variables to aeroval config file",
        action="store_true",
    )
    group_menujson.add_argument(
        "--adjustmenujson",
        help=" <aeroval cfgfile> <path to menu.json>; adjust order of menu.json to aeroval config file",
        action="store_true",
    )
    group_menujson.add_argument(
        "--adjustheatmap",
        help=" <aeroval cfgfile> <path to glob_*_monthly.json>; adjust order of menu.json to aeroval config file",
        action="store_true",
    )
    # group_menujson.add_argument("-a", "--adjustall", help="adjust order of all models/variables to aeroval config file", nargs=2,
    #                             metavar=("<aeroval cfgfile>", "<path to menu.json>"))

    args = parser.parse_args()
    options = {}
    if args.adjustall:
        options["adjustall"] = True
    else:
        options["adjustall"] = False

    if args.adjustmenujson:
        options["adjustmenujson"] = True
    else:
        options["adjustmenujson"] = False

    if args.adjustheatmap:
        options["adjustheatmap"] = True
    else:
        options["adjustheatmap"] = False

    if args.files:
        options["files"] = args.files

    if args.jsonrunscript:
        options["jsonrunscript"] = args.jsonrunscript

    if args.verbose:
        options["verbose"] = True
    else:
        options["verbose"] = False

    if args.noqsub:
        options["noqsub"] = True
    else:
        options["noqsub"] = False

    if args.env:
        options["conda_env_name"] = args.env

    if args.queue:
        options["qsub_queue_name"] = args.queue
    else:
        options["qsub_queue_name"] = QSUB_QUEUE_NAME

    if args.queue_user:
        options["qsub_user"] = args.queue_user
    else:
        options["qsub_user"] = QSUB_USER

    if args.tempdir:
        options["tempdir"] = Path(args.tempdir)

    if args.remotetempdir:
        options["remotetempdir"] = Path(args.remotetempdir)

    if args.cfgvar:
        options["cfgvar"] = args.cfgvar

    if args.json_basedir:
        options["json_basedir"] = args.json_basedir

    if args.coldata_basedir:
        options["coldata_basedir"] = args.coldata_basedir

    if args.io_aux_file:
        options["io_aux_file"] = args.io_aux_file

    if args.combinedirs:
        options["combinedirs"] = True
    else:
        options["combinedirs"] = False

    if args.localhost:
        options["localhost"] = True
    else:
        options["localhost"] = False

    if args.outdir:
        options["outdir"] = Path(args.outdir)

    # make sure that if -c switch is given also the -o option is there
    if options["combinedirs"] and "outdir" not in options:
        error_str = """Error: -c switch given but no output directory defined. 
Please add an output directory using the -o switch."""
        print(error_str)
        sys.exit(1)

    if options["localhost"]:
        info_str = "INFO: starting queue submission on localhost (-l flag is set)."
        print(info_str)

    if (
        not options["combinedirs"]
        and not options["adjustmenujson"]
        and not options["adjustheatmap"]
        and not options["adjustall"]
    ):
        # create file for the queue
        runfiles = prep_files(options)
        if options["noqsub"] and options["verbose"]:
            # just print the to be run files
            for _runfile in runfiles:
                print(f"created {_runfile}")
            pass
        else:
            run_queue(runfiles, submit_flag=(not options["noqsub"]), options=options)

    elif options["adjustmenujson"]:
        # adjust menu.json
        adjust_menujson(
            options["files"][1],
            config_file=options["files"][0],
            cfgvar=options["cfgvar"],
        )

    elif options["adjustheatmap"]:
        # adjust heatmap file
        adjust_heatmapfile(
            options["files"][1:],
            config_file=options["files"][0],
            cfgvar=options["cfgvar"],
        )

    elif options["adjustall"]:
        # combine all necessary adjustments:
        aeroval_conf_file = options["files"][0]
        menu_json_file = options["files"][1]
        json_path = Path(menu_json_file).parent

        cfg = read_config_var(aeroval_conf_file, options["cfgvar"])

        # adjust menu.json
        adjust_menujson(
            menu_json_file,
            cfg=cfg,
        )

        # adjust heatmap file
        hm_files = []
        for _mask in AEROVAL_HEATMAP_FILES_MASK:
            hm_files.extend([x for x in json_path.glob(_mask)])
        adjust_heatmapfile(
            sorted(hm_files),
            cfg=cfg,
        )
        # for _file in hm_files:
        #     adjust_heatmapfile(_file, cfg=cfg, )

        hm_ts_files = []
        # adjust hm/ts files
        for _mask in AEROVAL_HEATMAP_TS_FILES_MASK:
            hm_ts_files.extend([x for x in json_path.glob(_mask)])

        adjust_hm_ts_file(hm_ts_files, cfg=cfg)

    else:
        result = combine_output(options)


if __name__ == "__main__":
    main()
