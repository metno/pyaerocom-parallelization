#!/usr/bin/env python3
"""
cache file generator CLI for pyaerocom

for usage via the PPI queues
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from tempfile import mkdtemp

from aeroval_parallelize.cache_tools import (
    CONDA_ENV,
    QSUB_DIR,
    QSUB_HOST,
    QSUB_QUEUE_NAME,
    QSUB_SCRIPT_START,
    QSUB_SHORT_QUEUE_NAME,
    QSUB_USER,
    RND,
    TMP_DIR,
    run_queue,
    write_script,
    DEFAULT_CACHE_RAM,
)


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
{colors['UNDERLINE']}start cache creation serially on localhost{colors['END']}
{script_name} --vars concpm10 concpm25 -o EEAAQeRep.v2

{colors['UNDERLINE']}start cache creation parallel on qsub host (current host is NOT qsub host){colors['END']}
{script_name} --qsub --vars ang4487aer od550aer -o AeronetSunV3Lev2.daily

{colors['UNDERLINE']}start cache creation parallel on qsub host (current host IS qsub host){colors['END']}
{script_name} -l --qsub --vars concpm10 concpm25 vmro3 concno2 -o EEAAQeRep.NRT

    """,
    )
    parser.add_argument("--vars", help="variable name(s) to cache", nargs="+")
    parser.add_argument("-o", "--obsnetworks", help="obs networks(s) names to cache", nargs="+")
    parser.add_argument("-v", "--verbose", help="switch on verbosity", action="store_true")

    parser.add_argument(
        "-e",
        "--env",
        help=f"conda env used to run the aeroval analysis; defaults to {CONDA_ENV}",
        default=CONDA_ENV,
    )
    parser.add_argument(
        "--tempdir",
        help=f"directory for temporary files; defaults to {TMP_DIR}",
        default=TMP_DIR,
    )

    parser.add_argument(
        "-l", "--localhost", help="start queue submission on localhost", action="store_true"
    )
    parser.add_argument(
        "-p",
        "--printobsnetworks",
        help="just print the names of the supported obs network",
        action="store_true",
    )
    group_queue_opts = parser.add_argument_group("queue options", "options for running on PPI")
    group_queue_opts.add_argument(
        "--queue",
        help=f"queue name to submit the jobs to; defaults to {QSUB_SHORT_QUEUE_NAME}",
        default=QSUB_SHORT_QUEUE_NAME,
    )
    group_queue_opts.add_argument(
        "--qsub-host", help=f"queue submission host; defaults to {QSUB_HOST}", default=QSUB_HOST
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
        help="copy all files to qsub host, but do not submit to queue",
        action="store_true",
    )
    group_queue_opts.add_argument(
        "--remotetempdir",
        help=f"directory for temporary files on qsub node; defaults to {TMP_DIR}",
        default=TMP_DIR,
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

    if args.printobsnetworks:
        from pyaerocom import const

        supported_obs_networks = [
            const.AERONET_SUN_V2L15_AOD_DAILY_NAME,
            const.AERONET_SUN_V2L15_AOD_ALL_POINTS_NAME,
            const.AERONET_SUN_V2L2_AOD_DAILY_NAME,
            const.AERONET_SUN_V2L2_AOD_ALL_POINTS_NAME,
            const.AERONET_SUN_V2L2_SDA_DAILY_NAME,
            const.AERONET_SUN_V2L2_SDA_ALL_POINTS_NAME,
            const.AERONET_INV_V2L15_DAILY_NAME,
            const.AERONET_INV_V2L15_ALL_POINTS_NAME,
            const.AERONET_INV_V2L2_DAILY_NAME,
            const.AERONET_INV_V2L2_ALL_POINTS_NAME,
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
            const.MARCO_POLO_NAME,
            const.AIR_NOW_NAME,
        ]
        # since the IPCForest data is not in main-dev branch yet
        try:
            supported_obs_networks += [const.IPCFORESTS_NAME]
        except AttributeError:
            pass

        print(f"supported observational networks:\n", *sorted(supported_obs_networks), sep="\n")
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

    if args.env:
        options["conda_env_name"] = args.env

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

    if args.remotetempdir:
        options["remotetempdir"] = Path(args.remotetempdir)

    if args.localhost:
        options["localhost"] = True
    else:
        options["localhost"] = False

    # generate cache files locally
    scripts_to_run = []
    # create tmp dir if needed
    tempdir = Path(mkdtemp(dir=options["tempdir"]))

    for obs_network in options["obsnetworks"]:
        for var in options["vars"]:
            # write python file
            outfile = tempdir.joinpath(f"pya_{rnd}_caching_{obs_network}_{var}.py")
            write_script(outfile, var=var, obsnetwork=obs_network)
            scripts_to_run.append(outfile)

    if options["localhost"] or options["qsub"]:
        # run via queue, either on localhost or qsub submit host
        run_queue(
            scripts_to_run,
            submit_flag=(not options["dry_qsub"]),
            qsub_dir=options["qsub_dir"],
            options=options,
            qsub_queue=options["qsub_queue_name"],
        )
    # elif not options["localhost"] and options["qsub"] and options["dry_qsub"]:
    #     run_queue(scripts_to_run, submit_flag=(options["qsub"]), qsub_dir=options["qsub_dir"], options=options)
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
