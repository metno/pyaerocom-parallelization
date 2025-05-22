#!/usr/bin/env python3
"""
command line interface for parallelisation for aeroval processing

- create several aeroval config files from one input config
  (per model and per obs network for now)
- submit these configs to the GridEngine queue

"""
from __future__ import annotations

import jsonpickle
import logging

import argparse
import os
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
    DEFAULT_CACHE_RAM,
    DEFAULT_ANA_RAM,
    DEFAULT_ASSEMBLY_RAM,
    PICKLE_JSON_EXT,
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
    ENV_MODULE_NAME,
)

CACHE_CREATION_CMD = ["pyaerocom_cachegen"]
RUN_PYARO_CACHING = True


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

{colors['UNDERLINE']}submit jobs to queue; parameters as defaults:{colors['END']}
    {script_name} <cfg-file>

{colors['UNDERLINE']}do not submit jobs to queue (dry-run):{colors['END']}
    {script_name} --dry-qsub <cfg-file>

{colors['UNDERLINE']}submit jobs to queue; use special module:{colors['END']}
    {script_name} -m /modules/MET/rhel8/user-modules/fou-kl/aerotools/pya-v2024.03.conda <cfg-file>

{colors['UNDERLINE']}run just the cache file generation:{colors['END']}
    {script_name} --cachegen-only <cfg-file>

{colors['UNDERLINE']}set data directories and submit to queue:{colors['END']}
    {script_name} --json_basedir /tmp/data --coldata_basedir /tmp/coldata --io_aux_file /tmp/gridded_io_aux.py <cfg-file>

{colors['UNDERLINE']}assemble aeroval data after a parallel run has been finished:{colors['END']}
    {colors['BOLD']}The output directory needs to be the target experiment's output path ! {colors['END']}
    {script_name} -c -o <output directory> <input directories>
    {script_name} -c -o ${{HOME}}/tmp/testing/IASI/ ${{HOME}}/tmpt39n2gp_*

{colors['UNDERLINE']}adjust all variable and model orders to the one given in a aeroval config file:{colors['END']}
    {script_name} --adjustall <aeroval-cfg-file> <path to menu.json>
    {script_name} --adjustall /tmp/config/cfg_cams2-82_IFS_beta.py /tmp/data/testmerge_all/IFS-beta/menu.json

    
""",
    )
    parser.add_argument(
        "files",
        help="file(s) to read, directories to combine (if -c switch is used)",
        nargs="+",
    )

    parser.add_argument(
        "-v", "--verbose", help="switch on verbosity", action="store_true"
    )

    parser.add_argument(
        "-m",
        "--module",
        help=f"environment module to use; defaults to {ENV_MODULE_NAME}",
        default=ENV_MODULE_NAME,
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
        "--extract_obsconfigfile",
        help="extract obsconfig files and save to tmp path (mainly for testing)",
    )

    group_caching = parser.add_argument_group(
        "caching options", "options for cache file generation"
    )
    group_caching.add_argument(
        "--nocache",
        help="switch off cache generation before running aeroval",
        action="store_true",
    )

    group_caching.add_argument(
        "--cachegen-only",
        help="run the cache file generation only",
        action="store_true",
    )

    group_queue_opts = parser.add_argument_group(
        "queue options", "options for running on PPI"
    )
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
        "--queue-user", help=f"queue user; defaults to {QSUB_USER}"
    )
    group_queue_opts.add_argument(
        "--dry-qsub",
        help="do not submit to queue (all files created, but no submission)",
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
    group_queue_opts.add_argument(
        "--cacheram",
        help=f"RAM usage [GB] for cache queue jobs (defaults to {DEFAULT_CACHE_RAM}GB).",
        default=DEFAULT_CACHE_RAM,
    )
    group_queue_opts.add_argument(
        "--anaram",
        help=f"RAM usage [GB] for analysis queue jobs (defaults to {DEFAULT_ANA_RAM}GB).",
        default=DEFAULT_ANA_RAM,
    )
    group_queue_opts.add_argument(
        "--assemblyram",
        help=f"RAM usage [GB] for assembly queue jobs (defaults to {DEFAULT_ASSEMBLY_RAM}GB.",
        default=DEFAULT_ASSEMBLY_RAM,
    )

    group_assembly = parser.add_argument_group(
        "data assembly", "options for assembly of parallelizations output"
    )
    group_assembly.add_argument(
        "-o", "--outdir", help="output directory for experiment assembly"
    )
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

    if args.cachegen_only:
        options["cachegen_only"] = args.cachegen_only
    else:
        options["cachegen_only"] = False

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

    if args.extract_obsconfigfile:
        options["extract_obsconfigfile"] = True
    else:
        options["extract_obsconfigfile"] = False

    if args.dry_qsub:
        options["dry_qsub"] = True
    else:
        options["dry_qsub"] = False

    if args.module:
        options["env_mod"] = args.module
    else:
        options["env_mod"] = ENV_MODULE_NAME

    if args.queue:
        options["qsub_queue_name"] = args.queue

    if args.cache_queue:
        options["qsub_cache_queue_name"] = args.cache_queue

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

    if args.cacheram:
        options["cacheram"] = str(args.cacheram)
    else:
        options["cacheram"] = str(DEFAULT_CACHE_RAM)

    if args.anaram:
        options["anaram"] = args.anaram
    else:
        options["anaram"] = DEFAULT_ANA_RAM

    if args.assemblyram:
        options["assemblyram"] = args.assemblyram
    else:
        options["assemblyram"] = DEFAULT_ASSEMBLY_RAM

    if args.tempdir:
        options["tempdir"] = Path(args.tempdir)

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

    if args.outdir:
        options["outdir"] = Path(args.outdir)

    # make sure that if -c switch is given also the -o option is there
    if options["combinedirs"] and "outdir" not in options:
        error_str = """Error: -c switch given but no output directory defined. 
Please add an output directory using the -o switch."""
        print(error_str)
        sys.exit(1)

    if options["extract_obsconfigfile"]:
        # just create the obsconfig files and exit
        raise NotImplementedError

    if (
        not options["combinedirs"]
        and not options["adjustmenujson"]
        and not options["adjustheatmap"]
        and not options["adjustall"]
    ):
        # create aeroval config file for the queue
        # for now one for each model and Obsnetwork combination
        runfiles, cache_job_id_mask, json_run_dirs, tempdir = prep_files(options)
        # host_str = f"{options['qsub_user']}@{options['qsub_host']}"
        obs_conf_flag = False
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
                    options["hold_jid"][_aeroval_file] = cache_job_id_mask[
                        _aeroval_file
                    ]
                except KeyError:
                    options["hold_jid"] = {}
                    options["hold_jid"][_aeroval_file] = cache_job_id_mask[
                        _aeroval_file
                    ]

                conf_info = get_config_info(_aeroval_file, options["cfgvar"])

                for obs_net_key in conf_info:
                    # obs_net_key = next(iter(conf_info))
                    if (
                        obs_net_key in submitted_obs_nets
                    ):  # conf_info always has just one key
                        # Obs net could have been used before, but not necessarily all vars
                        # the following creates a list of
                        if all(
                            item in submitted_obs_nets[obs_net_key]
                            for item in conf_info[obs_net_key]["obs_vars"]
                        ):
                            continue
                        else:
                            submitted_obs_nets[obs_net_key] += list(
                                set(submitted_obs_nets[obs_net_key])
                                - set(conf_info[obs_net_key]["obs_vars"])
                            )
                    else:
                        try:
                            submitted_obs_nets[obs_net_key] = deepcopy(
                                conf_info[obs_net_key]["obs_vars"]
                            )
                        except KeyError:
                            submitted_obs_nets[obs_net_key].update(
                                deepcopy(conf_info[obs_net_key]["obs_vars"])
                            )

                        # create pyaro config, if necessary
                        if "pyaro_config" in conf_info[obs_net_key]:
                            obs_conf_flag = RUN_PYARO_CACHING
                            if obs_conf_flag:
                                obs_conf_file = Path(tempdir).joinpath(
                                    f"pya_{rnd}_caching_{obs_net_key}{PICKLE_JSON_EXT}"
                                )
                                if os.path.exists(obs_conf_file):
                                    continue
                                else:
                                    print(f"writing file {obs_conf_file}")
                                    json_string = jsonpickle.encode(
                                        conf_info[obs_net_key]["pyaro_config"]
                                    )
                                    with open(
                                        obs_conf_file, "w", encoding="utf-8"
                                    ) as j:
                                        j.write(json_string)
                        else:
                            obs_conf_flag = False
                            obs_conf_file = None

                    # cache creation is started via the command line for simplicity
                    cmd_arr = [*CACHE_CREATION_CMD]
                    # if options["localhost"]:
                    #     cmd_arr += ["-l"]
                    if "env_mod" in options and options["env_mod"] != ENV_MODULE_NAME:
                        cmd_arr += ["-m", options["env_mod"]]
                    # append queue options
                    queue_opts = [
                        "--queue",
                        options["qsub_cache_queue_name"],
                        "--ram",
                        options["cacheram"],
                        "--queue-user",
                        options["qsub_user"],
                        "--qsub-id",
                        str(rnd),
                        "--qsub-dir",
                        # emulates the qsub tempdir from the later run_queue method
                        # the goal is to use always just one qsub directory for the cache
                        # file generation and the aeroval parallelization
                        f"{tempdir}",
                    ]
                    if obs_conf_flag:
                        cmd_arr += ["--obsconfigfile", obs_conf_file]
                    # qsub or dry-qsub?
                    if options["dry_qsub"]:
                        queue_opts += ["--dry-qsub"]
                    else:
                        queue_opts += ["--qsub"]
                    cmd_arr += queue_opts
                    # cmd_tmp_arr = deepcopy(cmd_arr)
                    static_opts = [
                        "--vars",
                        *conf_info[obs_net_key]["obs_vars"],
                        "-o",
                        obs_net_key,
                    ]
                    cmd_arr += static_opts

                    print(f"running command {' '.join(map(str, cmd_arr))}...")
                    sh_result = subprocess.run(cmd_arr, capture_output=True)
                    print(f"{sh_result.stdout}")
                    if sh_result.returncode != 0:
                        continue
                    else:
                        print("success...")

        if options["dry_qsub"] and options["verbose"]:
            # just print the to be run files
            for _runfile in runfiles:
                print(f"created {_runfile}")
        elif options["cachegen_only"]:
            print("cache file generation only was requested. Exiting.")
            return
        else:
            run_queue(
                runfiles,
                submit_flag=(not options["dry_qsub"]),
                qsub_queue=options["qsub_queue_name"],
                qsub_dir=tempdir,
                options=options,
            )
            conf = read_config_var(config_file=runfiles[0], cfgvar=options["cfgvar"])
            # now add jobs for data assembly and json file reordering
            # create a data dict with the assembly directory as key and the directories to
            # assemble as list of values
            # assembly_paths = [Path(json_run_dirs[x]).parent for x in range(len(json_run_dirs))]
            wds = {}
            # the following returns a list of unique data assembly paths
            for json_dir in json_run_dirs:
                _dir = str(
                    Path.joinpath(
                        Path(json_dir).parent, conf["proj_id"], conf["exp_id"]
                    )
                )
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
                    ram=options["assemblyram"],
                    queue_name=options["qsub_queue_name"],
                    module=options["env_mod"],
                )
                qsub_start_file_name = Path.joinpath(
                    Path(tempdir), f"pya_{rnd}_data_merging.run"
                )

                # Now add the reordering job. Just add that to the pure assembly job
                # since it's a serial operation anyway
                menu_json_file = Path.joinpath(Path(out_dir), "menu.json")
                aeroval_config_file = Path(options["files"][0]).resolve()
                reorder_cmd_arr = [
                    "aeroval_parallelize",
                    "--adjustall",
                    f"'{aeroval_config_file}'",
                    f"'{menu_json_file}'",
                ]
                reorder_cmd_str = " ".join(map(str, reorder_cmd_arr))

                assembly_script_str += f"""echo "starting {reorder_cmd_str} ..." >> ${{logfile}}
{reorder_cmd_str} >> ${{logfile}} 2>&1"""

                with open(qsub_start_file_name, "w") as f:
                    f.write(assembly_script_str)
                run_queue_simple(
                    [qsub_start_file_name],
                    submit_flag=(not options["dry_qsub"]),
                    qsub_dir=tempdir,
                    options=options,
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
