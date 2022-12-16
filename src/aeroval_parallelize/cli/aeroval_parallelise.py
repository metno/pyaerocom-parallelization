#!/usr/bin/env python3
"""
command line interface for parallelisation for aeroval processing

- create several aeroval config files from one input config
  (per model and per obs network for now)
- submit these configs to the GridEngine queue

"""
from __future__ import annotations

import argparse
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

from aeroval_parallelize.const import (
    CONDA_ENV,
    CP_COMMAND,
    DEFAULT_CFG_VAR,
    QSUB_DIR,
    QSUB_HOST,
    QSUB_LOG_DIR,
    QSUB_NAME,
    QSUB_QUEUE_NAME,
    QSUB_SHORT_QUEUE_NAME,
    QSUB_USER,
    REMOTE_CP_COMMAND,
    RND,
    RUN_UUID,
    TMP_DIR,
    USER,
)
from aeroval_parallelize.tools import (  # CONDA_ENV,; JSON_RUNSCRIPT,; QSUB_HOST,; QSUB_QUEUE_NAME,; QSUB_USER,; TMP_DIR,; RND,; RUN_UUID,
    AEROVAL_HEATMAP_FILES_MASK,
    AEROVAL_HEATMAP_TS_FILES_MASK,
    JSON_RUNSCRIPT,
    adjust_heatmapfile,
    adjust_hm_ts_file,
    adjust_menujson,
    combine_output,
    get_assembly_job_str,
    get_config_info,
    prep_files,
    read_config_var,
    run_queue,
    run_queue_simple,
)

CACHE_CREATION_CMD = ["pyaerocom_cachegen"]


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
    {colors['BOLD']} The output directory needs to be the target experiment's output path ! {colors['END']}
    {script_name} -c -o <output directory> <input directories>
    {script_name} -c -o ${{HOME}}/tmp/testing/IASI/ ${{HOME}}/tmpt39n2gp_*

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
    group_caching = parser.add_argument_group(
        "caching options", "options for cache file generation"
    )
    group_caching.add_argument(
        "--nocache",
        help="switch off cache generation before running aeroval",
        action="store_true",
    )

    group_queue_opts = parser.add_argument_group("queue options", "options for running on PPI")
    group_queue_opts.add_argument(
        "--queue",
        help=f"queue name to submit the jobs to; defaults to {QSUB_QUEUE_NAME}",
        default=QSUB_QUEUE_NAME,
    )
    group_queue_opts.add_argument(
        "--cache_queue",
        help=f"queue name to submit the caching jobs to; defaults to {QSUB_SHORT_QUEUE_NAME}",
        default=QSUB_SHORT_QUEUE_NAME,
    )
    group_queue_opts.add_argument(
        "--qsub-host", help=f"queue submission host; defaults to {QSUB_HOST}", default=QSUB_HOST
    )
    group_queue_opts.add_argument("--queue-user", help=f"queue user; defaults to {QSUB_USER}")
    group_queue_opts.add_argument(
        "--noqsub",
        help="do not submit to queue (all files created and copied, but no submission)",
        action="store_true",
    )
    group_queue_opts.add_argument(
        "--qsub-id",
        help="id under which the qsub commands will be run. Needed only for automation.",
    )
    group_queue_opts.add_argument(
        "--qsub-dir",
        help=f"directory under which the qsub scripts will be stored. defaults to {QSUB_DIR}, needs to be on fs mounted by all queue hosts.",
    )

    group_assembly = parser.add_argument_group(
        "data assembly", "options for assembly of parallisations output"
    )
    group_assembly.add_argument("-o", "--outdir", help="output directory for experiment assembly")
    group_assembly.add_argument(
        "-c",
        "--combinedirs",
        help="combine the output of a parallel runs; MUST INCLUDE <project dir>/<experiment dir>!!",
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

    args = parser.parse_args()
    options = {}
    if args.adjustall:
        options["adjustall"] = True
    else:
        options["adjustall"] = False

    if args.nocache:
        options["nocache"] = True
    else:
        options["nocache"] = False

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

    if args.cache_queue:
        options["qsub_cache_queue_name"] = args.cache_queue

    if args.qsub_host:
        options["qsub_host"] = args.qsub_host
    else:
        options["qsub_host"] = QSUB_HOST

    if args.queue_user:
        options["qsub_user"] = args.queue_user
    else:
        options["qsub_user"] = QSUB_USER

    if args.qsub_dir:
        options["qsub_dir"] = args.qsub_dir
    else:
        options["qsub_dir"] = QSUB_DIR

    if args.qsub_id:
        options["qsub_id"] = args.qsub_id
        rnd = options["qsub_id"]
    else:
        options["qsub_id"] = RND
        rnd = RND

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
        # create aeroval config file for the queue
        # for now one for each model and Obsnetwork combination
        runfiles, cache_job_id_mask, json_run_dirs = prep_files(options)
        host_str = f"{options['qsub_user']}@{options['qsub_host']}"
        if not options["nocache"]:
            # CREATE CACHE
            # now start cache file generation using the command line for simplicity

            # store already submitted obs networks
            submitted_obs_nets = {}
            for _aeroval_file in runfiles:
                # create jobs for cache file generation first and add the wait for them to the qsub parameters
                # add waiting for all cache file generation scripts for now
                # options["hold_jid"] = "create_cache_*"
                try:
                    options["hold_jid"][_aeroval_file] = cache_job_id_mask[_aeroval_file]
                except KeyError:
                    options["hold_jid"] = {}
                    options["hold_jid"][_aeroval_file] = cache_job_id_mask[_aeroval_file]

                conf_info = get_config_info(_aeroval_file, options["cfgvar"])
                obs_net_key = next(iter(conf_info))
                if obs_net_key in submitted_obs_nets:  # conf info always has just one key
                    # Obs net could have been used before, but not necessarily all vars
                    # the following creates a list of
                    if all(
                        item in submitted_obs_nets[obs_net_key] for item in conf_info[obs_net_key]
                    ):
                        continue
                    else:
                        submitted_obs_nets[obs_net_key] += list(
                            set(submitted_obs_nets[obs_net_key]) - set(conf_info[obs_net_key])
                        )
                else:
                    submitted_obs_nets.update(deepcopy(conf_info))
                # TODO: add conda env  options

                # cache creation is started via the command line for simplicity
                cmd_arr = [*CACHE_CREATION_CMD]
                if options["localhost"]:
                    cmd_arr += ["-l"]
                # append queue options
                queue_opts = [
                    "--qsub",
                    "--queue",
                    options["qsub_cache_queue_name"],
                    "--queue-user",
                    options["qsub_user"],
                    "--qsub-id",
                    str(rnd),
                    "--qsub-dir",
                    options["qsub_dir"],
                ]
                if options["noqsub"]:
                    queue_opts += ["--dry-qsub"]
                cmd_arr += queue_opts
                for obs_net in conf_info:
                    cmd_tmp_arr = cmd_arr
                    static_opts = [
                        "--vars",
                        *conf_info[obs_net],
                        "-o",
                        obs_net,
                    ]
                    cmd_tmp_arr += static_opts

                    print(f"running command {' '.join(map(str, cmd_tmp_arr))}...")
                    sh_result = subprocess.run(cmd_tmp_arr, capture_output=True)
                    print(f"{sh_result.stdout}")
                    if sh_result.returncode != 0:
                        continue
                    else:
                        print("success...")

        if options["noqsub"] and options["verbose"]:
            # just print the to be run files
            for _runfile in runfiles:
                print(f"created {_runfile}")
            pass
        else:
            run_queue(runfiles, submit_flag=(not options["noqsub"]), options=options)
            conf = read_config_var(config_file=runfiles[0], cfgvar=options["cfgvar"])
            # now add jobs for data assembly and json file reordering
            # create a data dict with the assembly directory as key and the directories to
            # assemble as list of values
            # assembly_paths = [Path(json_run_dirs[x]).parent for x in range(len(json_run_dirs))]
            wds = {}
            # the following returns a list of unique data assembly paths
            for json_dir in json_run_dirs:
                _dir = str(Path.joinpath(Path(json_dir).parent, conf["proj_id"], conf["exp_id"]))
                # _dir = str(Path.joinpath(Path(json_dir).parent, conf["proj_id"]))
                # _dir = str(Path(json_dir).parent)
                try:
                    wds[_dir].append(json_dir)
                except KeyError:
                    wds[_dir] = []
                    wds[_dir].append(json_dir)
            for out_dir in wds:
                assembly_script_str = get_assembly_job_str(
                    out_dir=out_dir,
                    in_dirs=wds[out_dir],
                    job_id=rnd,
                    wd=Path(out_dir).parent.parent,
                )
                qsub_start_file_name = Path.joinpath(
                    Path(runfiles[0]).parent, f"pya_{rnd}_data_merging.run"
                )

                # Now add the reordering job. Just add that to the pure assembly job
                # since it's a serial operation anyway
                menu_json_file = Path.joinpath(Path(out_dir), "menu.json")
                aeroval_config_file = Path(options["files"][0]).resolve()
                reorder_cmd_arr = [
                    "aeroval_parallelize",
                    "--adjustall",
                    f"{aeroval_config_file}",
                    menu_json_file,
                ]
                reorder_cmd_str = " ".join(map(str, reorder_cmd_arr))

                assembly_script_str += f"""echo "starting {reorder_cmd_str} ..." >> ${{logfile}}
{reorder_cmd_str} >> ${{logfile}} 2>&1"""

                with open(qsub_start_file_name, "w") as f:
                    f.write(assembly_script_str)
                run_queue_simple(
                    [qsub_start_file_name], submit_flag=(not options["noqsub"]), options=options
                )

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

        hm_ts_files = []
        # adjust hm/ts files
        for _mask in AEROVAL_HEATMAP_TS_FILES_MASK:
            hm_ts_files.extend([x for x in json_path.glob(_mask)])

        adjust_hm_ts_file(hm_ts_files, cfg=cfg)

    else:

        result = combine_output(options)


if __name__ == "__main__":
    main()
