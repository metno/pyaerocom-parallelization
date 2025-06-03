#!/usr/bin/env python3
"""
cache file generator CLI for pyaerocom

for usage via the PPI queues
"""
from __future__ import annotations

import argparse
import os.path
import subprocess
import sys
from pathlib import Path
from tempfile import mkdtemp
import jsonpickle

from aeroval_parallelize.cache_tools import (
    QSUB_DIR,
    QSUB_SHORT_QUEUE_NAME,
    QSUB_USER,
    RND,
    TMP_DIR,
    run_queue,
    write_script,
    write_script_pyaro,
    DEFAULT_CACHE_RAM,
    ENV_MODULE_NAME,
)
from pyaerocom.io.pyaro.pyaro_config import PyaroConfig


def main():
    colors = {
        "PURPLE": "\033[95m",
        "CYAN": "\033[96m",
        "BOLD": "\033[1m",
        "UNDERLINE": "\033[4m",
        "END": "\033[0m",
    }

    script_name = Path(sys.argv[0]).name
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=f"command line interface to pyaerocom cache file generator {script_name}.",
        epilog=f"""{colors['BOLD']}Example usages:{colors['END']}
{colors['UNDERLINE']}start cache generation serially{colors['END']}
{script_name} --vars concpm10 concpm25 -o EEAAQeRep.v2
{colors['UNDERLINE']}with pyaro config file{colors['END']}
{script_name} --vars concpm10 concpm25 -o EEAAQeRep.v2 --obsconfigfile <path to picklejson file>

{colors['UNDERLINE']}dry run cache generation for queue job{colors['END']}
{script_name} --dry-qsub --vars ang4487aer od550aer -o AeronetSunV3Lev2.daily

{colors['UNDERLINE']}use special module at queue run ({colors['BOLD']}full module path needed!{colors['END']})
{script_name} -m /modules/MET/rhel8/user-modules/fou-kl/aerotools/pya-v2024.03 --vars ang4487aer od550aer -o AeronetSunV3Lev2.daily

{colors['UNDERLINE']}start cache generation parallel on PPI queue{colors['END']}
{script_name} --qsub --vars ang4487aer od550aer -o AeronetSunV3Lev2.daily
{script_name} --qsub --vars concpm10 concpm25 vmro3 concno2 -o EEAAQeRep.NRT

    """,
    )
    parser.add_argument("--vars", help="variable name(s) to cache", nargs="+")
    parser.add_argument(
        "--obsconfigfile", help="picklejson config file for pyaro config", nargs=1
    )
    parser.add_argument(
        "-o", "--obsnetworks", help="obs networks(s) names to cache", nargs="+"
    )
    parser.add_argument(
        "-v", "--verbose", help="switch on verbosity", action="store_true"
    )

    parser.add_argument(
        "--tempdir",
        help=f"directory for temporary files; defaults to {TMP_DIR}",
        default=TMP_DIR,
    )

    parser.add_argument(
        "-m",
        "--module",
        help=f"environment module to use; defaults to {ENV_MODULE_NAME}",
        default=ENV_MODULE_NAME,
    )
    parser.add_argument(
        "-p",
        "--printobsnetworks",
        help="just print the names of the supported obs network",
        action="store_true",
    )
    group_queue_opts = parser.add_argument_group(
        "queue options", "options for running on PPI"
    )
    group_queue_opts.add_argument(
        "--queue",
        help=f"queue name to submit the jobs to; defaults to {QSUB_SHORT_QUEUE_NAME}",
        default=QSUB_SHORT_QUEUE_NAME,
    )
    group_queue_opts.add_argument(
        "--queue-user", help=f"queue user; defaults to {QSUB_USER}", default=QSUB_USER
    )
    group_queue_opts.add_argument(
        "--qsub", help="submit to queue using the qsub command", action="store_true"
    )
    group_queue_opts.add_argument(
        "--qsub-id",
        help="id under which the qsub commands will be run. Needed only for automation.",
    )
    group_queue_opts.add_argument(
        "--qsub-dir",
        help=f"directory under which the qsub scripts will be stored. defaults to {QSUB_DIR}, needs to be on fs mounted by all queue hosts.",
        default=QSUB_DIR,
    )
    group_queue_opts.add_argument(
        "--dry-qsub",
        help="create all files for qsub, but do not submit to queue",
        action="store_true",
    )
    group_queue_opts.add_argument(
        "-s",
        "--submission-dir",
        help=f"directory submission scripts",
    )
    group_queue_opts.add_argument(
        "-r",
        "--ram",
        help=f"RAM usage [GB] for queue",
    )

    args = parser.parse_args()
    options = {}
    if args.vars:
        options["vars"] = args.vars

    if args.obsconfigfile:
        options["obsconfigfile"] = args.obsconfigfile[0]
        if os.path.exists(options["obsconfigfile"]):
            with open(options["obsconfigfile"], "r") as f:
                json_str = f.read()
            obsconf = jsonpickle.decode(json_str)
            if isinstance(obsconf, PyaroConfig):
                obsconf = PyaroConfig.from_dict(obsconf)

    if args.printobsnetworks:
        from pyaerocom import const

        supported_obs_networks = [
            const.AERONET_SUN_V3L15_AOD_DAILY_NAME,
            const.AERONET_SUN_V3L15_AOD_ALL_POINTS_NAME,
            const.AERONET_SUN_V3L2_AOD_DAILY_NAME,
            const.AERONET_SUN_V3L2_AOD_ALL_POINTS_NAME,
            const.AERONET_SUN_V3L15_SDA_DAILY_NAME,
            const.AERONET_SUN_V3L15_SDA_ALL_POINTS_NAME,
            const.AERONET_SUN_V3L2_SDA_DAILY_NAME,
            const.AERONET_SUN_V3L2_SDA_ALL_POINTS_NAME,
            const.AERONET_INV_V3L15_DAILY_NAME,
            const.AERONET_INV_V3L2_DAILY_NAME,
            const.EBAS_MULTICOLUMN_NAME,
            const.EEA_NRT_NAME,
            const.EEA_V2_NAME,
            const.EARLINET_NAME,
            const.AIR_NOW_NAME,
            const.ICPFORESTS_NAME,
        ]
        # adjust to newer obs network names
        try:
            supported_obs_networks += [const.MEP_NAME]
        except AttributeError:
            pass

        try:
            supported_obs_networks += [const.CNEMC_NAME]
        except AttributeError:
            pass

        print(
            f"supported observational networks:\n",
            *sorted(supported_obs_networks),
            sep="\n",
        )
        sys.exit(0)

    if args.obsnetworks:
        options["obsnetworks"] = args.obsnetworks

    if args.verbose:
        options["verbose"] = True
    else:
        options["verbose"] = False

    if args.qsub:
        options["qsub"] = True
    else:
        options["qsub"] = False

    if args.module:
        options["env_mod"] = args.module
    else:
        options["env_mod"] = ENV_MODULE_NAME

    if args.queue:
        options["qsub_queue_name"] = args.queue

    if args.dry_qsub:
        options["dry_qsub"] = True
    else:
        options["dry_qsub"] = False

    if args.queue_user:
        options["qsub_user"] = args.queue_user

    if args.qsub_dir:
        options["qsub_dir"] = args.qsub_dir

    if args.qsub_id:
        options["qsub_id"] = args.qsub_id
        rnd = options["qsub_id"]
    else:
        rnd = RND

    if args.ram:
        options["qsub_ram"] = args.ram
    else:
        options["qsub_ram"] = DEFAULT_CACHE_RAM

    if args.tempdir:
        options["tempdir"] = Path(args.tempdir)

    # generate cache files
    scripts_to_run = []
    # create tmp dir for script creation
    # to run on the queue these have to be in either on /home or another
    # generally available file system
    # for local run we just put them below /tmp
    if options["qsub"] or options["dry_qsub"]:
        if options["qsub_dir"] == QSUB_DIR:
            tempdir = Path(mkdtemp(dir=options["qsub_dir"]))
        else:
            tempdir = Path(options["qsub_dir"])

        use_module = True
    else:
        tempdir = Path(mkdtemp(dir=options["tempdir"]))
        use_module = False

    if "obsconfigfile" in options:
        if not "vars" in options:
            # this works only if the pyaerocom and pyaro variable names are different!
            # we might want to pass the vars from aeroval_parallelise therefore
            # But keep this for now for command line usage
            vars_to_process = list(set(obsconf.name_map.values()))
        else:
            vars_to_process = options["vars"]

        for var in vars_to_process:
            # write python file
            obs_network = obsconf.name

            outfile = tempdir.joinpath(f"pya_{rnd}_caching_{obs_network}_{var}.py")
            write_script_pyaro(
                outfile,
                var=var,
                obsnetwork=obs_network,
                use_module=use_module,
                conffile=options["obsconfigfile"],
            )
            print(f"Wrote pyaro {outfile}")
            scripts_to_run.append(outfile)
    else:
        for obs_network in options["obsnetworks"]:
            for var in options["vars"]:
                # write python file
                outfile = tempdir.joinpath(f"pya_{rnd}_caching_{obs_network}_{var}.py")
                write_script(
                    outfile, var=var, obsnetwork=obs_network, use_module=use_module
                )
                print(f"Wrote {outfile}")
                scripts_to_run.append(outfile)

    if options["qsub"] or options["dry_qsub"]:
        # run via queue, either on localhost or qsub submit host
        run_queue(
            scripts_to_run,
            submit_flag=(not options["dry_qsub"]),
            options=options,
            qsub_queue=options["qsub_queue_name"],
        )
    else:
        # run serially on localhost
        for _script in scripts_to_run:
            cmd_arr = [_script]
            print(f"running command {' '.join(map(str, cmd_arr))}...")
            sh_result = subprocess.run(cmd_arr)
            if sh_result.returncode != 0:
                continue
            else:
                print("success...")


if __name__ == "__main__":
    main()
