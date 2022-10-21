#!/usr/bin/env python3
"""
parallelisation for aeroval processing

- create several aeroval config files from one input config
  (per model and per obs network for now)
- submit these configs to the GridEngine queue

"""
from __future__ import annotations

import sys
from copy import deepcopy
from datetime import datetime
from fnmatch import fnmatch
from getpass import getuser
from importlib.machinery import SourceFileLoader
from pathlib import Path
from random import randint
from socket import gethostname
from tempfile import mkdtemp
from threading import Thread
from uuid import uuid4

import simplejson as json

from aeroval_parallelize.cache_tools import QSUB_SCRIPT_START
from aeroval_parallelize.const import (
    CONDA_ENV,
    CP_COMMAND,
    JSON_RUNSCRIPT_NAME,
    QSUB_DIR,
    QSUB_HOST,
    QSUB_LOG_DIR,
    QSUB_NAME,
    QSUB_QUEUE_NAME,
    QSUB_USER,
    REMOTE_CP_COMMAND,
    RND,
    RUN_UUID,
    TMP_DIR,
    USER,
)

# DEFAULT_CFG_VAR = "CFG"
# RUN_UUID = uuid4()
# RND = randint(0, 1e9)
# HOSTNAME = gethostname()
# USER = getuser()
# TMP_DIR = "/tmp"
# TMP_DIR = f"/home/{USER}/data/aeroval-local-web/data"

# JSON_RUNSCRIPT_NAME = "aeroval_run_json_cfg"
# qsub binary
# QSUB_NAME = "/usr/bin/qsub"
# qsub submission host
# QSUB_HOST = "ppi-clogin-b1.met.no"
# directory, where the files will bew transferred before they are run
# Needs to be on Lustre or home since /tmp is not shared between machines
# QSUB_DIR = f"/lustre/storeA/users/{USER}/submission_scripts"

# user name on the qsub host
# QSUB_USER = USER
# queue name
# QSUB_QUEUE_NAME = "research-el7.q"
# log directory
# QSUB_LOG_DIR = "/lustre/storeA/project/aerocom/logs/aeroval_logs/"

# some copy constants
# REMOTE_CP_COMMAND = ["scp", "-v"]
# CP_COMMAND = ["cp", "-v"]

# script start time
START_TIME = datetime.now().strftime("%Y%m%d_%H%M%S")

# assume that the script to run the aeroval json file is in the same directory as this script
# JSON_RUNSCRIPT = Path(Path(__file__).parent).joinpath(JSON_RUNSCRIPT_NAME)
JSON_RUNSCRIPT = JSON_RUNSCRIPT_NAME

# match for aeroval config file
AEROVAL_CONFIG_FILE_MASK = ["cfg_*.json"]

# match for heatmap files; the results of them are displayed according to their order
# in the file. Unfortunately the parallelisation mixes that up, so we need to reorder them after
# the assembly of the data files
AEROVAL_HEATMAP_FILES_MASK = ["hm/glob_stats_*.json"]

# filemask for ts files (tab=timeseries) in aeroval
# these also use the order in the json file for display and therefore need to be adjusted
AEROVAL_HEATMAP_TS_FILES_MASK = [
    "hm/ts/*.json",
]

# some constants for the merge operation
# files not noted here will be copied
# list of file masks to merge

MERGE_EXP_FILES_TO_COMBINE = [
    "ts/*.json",
    *AEROVAL_HEATMAP_TS_FILES_MASK,
    "menu.json",
    "ranges.json",
    "regions.json",
    "statistics.json",
    *AEROVAL_HEATMAP_FILES_MASK,
    *AEROVAL_CONFIG_FILE_MASK,
]
# list of file masks not to touch
MERGE_EXP_FILES_TO_EXCLUDE = []
# the config file need to be merged and have a special name
MERGE_EXP_CFG_FILES = ["cfg_*.json"]
# Name of conda env to use for running the aeroval analysis
CONDA_ENV = "pya_para"


def prep_files(options):
    """preprare the aeroval config files to run
    return a list of files

    """
    # returned list of runfiles
    runfiles = []
    # dict with the run filename as key and the corresponding cache creation mask
    cache_job_id_mask = {}

    for _file in options["files"]:
        # read aeroval config file
        if fnmatch(_file, "*.py"):
            foo = SourceFileLoader("bla", _file).load_module()
            # the following line does unfortunately not work since a module is not subscriptable
            # CFG = foo[options["cfgvar"]]
            # use getattr instead
            cfg = deepcopy(getattr(foo, options["cfgvar"]))

        elif fnmatch(_file, "*.json"):
            cfg = json.load(_file)
        else:
            print(f"skipping file {_file} due to wrong file extension")
            continue

        # create tmp dir
        tempdir = mkdtemp(dir=options["tempdir"])

        # make some adjustments to the config file
        # e.g. adjust the json_basedir and the coldata_basedir entries
        if "json_basedir" in options:
            cfg["json_basedir"] = options["json_basedir"]

        if "coldata_basedir" in options:
            cfg["coldata_basedir"] = options["coldata_basedir"]

        if "io_aux_file" in options:
            cfg["io_aux_file"] = options["io_aux_file"]

        # index for temporary data directories
        dir_idx = 1

        # determine if one of the obs is a superobs
        # if yes, we need to run the parts of the superobs together with
        # superobs
        # in that case we parallelise the model only for now
        no_superobs_flag = True
        out_cfg = deepcopy(cfg)
        for _obs in out_cfg["obs_cfg"]:
            try:
                if out_cfg["obs_cfg"][_obs]["is_superobs"]:
                    no_superobs_flag = False
                    # store the obs needed for superobs
                    superobs_obs = out_cfg["obs_cfg"][_obs]["obs_id"]
            except:
                pass

        for _model in cfg["model_cfg"]:
            out_cfg = deepcopy(cfg)
            out_cfg.pop("model_cfg", None)
            out_cfg["model_cfg"] = {}
            out_cfg["model_cfg"][_model] = cfg["model_cfg"][_model]

            if no_superobs_flag:
                out_cfg.pop("obs_cfg", None)
                for _obs_network in cfg["obs_cfg"]:
                    # cache file generation works with pyaerocom's obs network names
                    # and not the one of aeroval (those used in the web interface)
                    pya_obsid = cfg["obs_cfg"][_obs_network]["obs_id"]
                    out_cfg["obs_cfg"] = {}
                    out_cfg["obs_cfg"][_obs_network] = cfg["obs_cfg"][_obs_network]
                    # adjust json_basedir and coldata_basedir so that the different runs
                    # do not influence each other
                    out_cfg[
                        "json_basedir"
                    ] = f"{cfg['json_basedir']}/{Path(tempdir).parts[-1]}.{dir_idx:04d}"
                    out_cfg[
                        "coldata_basedir"
                    ] = f"{cfg['coldata_basedir']}/{Path(tempdir).parts[-1]}.{dir_idx:04d}"
                    cfg_file = Path(_file).stem
                    outfile = Path(tempdir).joinpath(f"{cfg_file}_{_model}_{_obs_network}.json")
                    # the parallelisation is based on obs network for now only, while the cache
                    # generation runs the variables in parallel already
                    cache_job_id_mask[outfile] = f"{QSUB_SCRIPT_START}_{pya_obsid}*"
                    print(f"writing file {outfile}")
                    with open(outfile, "w", encoding="utf-8") as j:
                        json.dump(out_cfg, j, ensure_ascii=False, indent=4)
                    dir_idx += 1
                    runfiles.append(outfile)
                    if options["verbose"]:
                        print(out_cfg)
            else:
                # adjust json_basedir and coldata_basedir so that the different runs
                # do not influence each other
                _obs_network = "allobs"
                out_cfg[
                    "json_basedir"
                ] = f"{cfg['json_basedir']}/{Path(tempdir).parts[-1]}.{dir_idx:04d}"
                out_cfg[
                    "coldata_basedir"
                ] = f"{cfg['coldata_basedir']}/{Path(tempdir).parts[-1]}.{dir_idx:04d}"
                cfg_file = Path(_file).stem
                outfile = Path(tempdir).joinpath(f"{cfg_file}_{_model}_{_obs_network}.json")
                cache_job_id_mask[outfile] = f"{QSUB_SCRIPT_START}_{_obs_network}*"
                print(f"writing file {outfile}")
                with open(outfile, "w", encoding="utf-8") as j:
                    json.dump(out_cfg, j, ensure_ascii=False, indent=4)
                dir_idx += 1
                runfiles.append(outfile)
                if options["verbose"]:
                    print(out_cfg)

    return runfiles, cache_job_id_mask


def get_runfile_str(
    file,
    queue_name=QSUB_QUEUE_NAME,
    script_name=None,
    wd=None,
    mail=f"{QSUB_USER}@met.no",
    logdir=QSUB_LOG_DIR,
    date=START_TIME,
    conda_env=CONDA_ENV,
    hold_pattern=None,
):
    """create list of strings with runfile for gridengine

    Parameters
    ----------
    hold_pattern
    file
    queue_name
    script_name
    wd
    mail
    logdir
    date
    conda_env
    """
    # create runfile

    if wd is None:
        wd = Path(file).parent

    if script_name is None:
        script_name = str(file.with_name(f"{file.stem}{'.run'}"))
    elif isinstance(script_name, Path):
        script_name = str(script_name)

    runfile_str = f"""#!/bin/bash -l
#$ -S /bin/bash
#$ -N {Path(file).stem}
#$ -q {queue_name}
#$ -pe shmem-1 1
#$ -wd {wd}
#$ -l h_rt=96:00:00
#$ -l s_rt=96:00:00
"""
    if mail is not None:
        runfile_str += f"#$ -M {mail}\n"
    runfile_str += f"""#$ -m abe
#$ -l h_vmem=20G
#$ -shell y
#$ -j y
#$ -o {logdir}/
#$ -e {logdir}/
"""
    if hold_pattern is not None and isinstance(hold_pattern, str):
        runfile_str += f"""#$ -hold_jid {hold_pattern}\n"""

    runfile_str += f"""
logdir="{logdir}/"
date="{date}"
logfile="${{logdir}}/${{USER}}.${{date}}.${{JOB_NAME}}.${{JOB_ID}}_log.txt"
__conda_setup=$('/modules/centos7/user-apps/aerocom/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)
if [ $? -eq 0 ]
then eval "$__conda_setup"
else
  echo conda not working! exiting...
  exit 1
fi
echo "Got $NSLOTS slots for job $SGE_TASK_ID." >> ${{logfile}}
module load aerocom/anaconda3-stable >> ${{logfile}} 2>&1
module list >> ${{logfile}} 2>&1
conda activate {conda_env} >> ${{logfile}} 2>&1
conda env list >> ${{logfile}} 2>&1
set -x
python --version >> ${{logfile}} 2>&1
pwd >> ${{logfile}} 2>&1
echo "starting {file} ..." >> ${{logfile}}
{str(JSON_RUNSCRIPT)} {str(file)} >> ${{logfile}} 2>&1

"""
    return runfile_str


def run_queue(
    runfiles: list[Path],
    qsub_host: str = QSUB_HOST,
    qsub_cmd: str = QSUB_NAME,
    qsub_dir: str = QSUB_DIR,
    qsub_user: str = QSUB_USER,
    qsub_queue: str = QSUB_QUEUE_NAME,
    submit_flag: bool = False,
    options: dict = {},
):
    """submit runfiles to the remote cluster

    # copy runfile to qsub host (subprocess.run)
    # create submission file (create locally, copy to qsub host (fabric)
    # create tmp directory on submission host (fabric)
    # submit submission file to queue (fabric)

    """

    import subprocess

    qsub_tmp_dir = Path.joinpath(Path(qsub_dir), f"qsub.{runfiles[0].parts[-2]}")

    for idx, _file in enumerate(runfiles):
        try:
            hold_pattern = options["hold_jid"][_file]
        except KeyError:
            hold_pattern = None
        # copy runfiles to qsub host if qsub host is not localhost
        if not options["localhost"]:
            # create tmp dir on qsub host; retain some parts
            host_str = f"{qsub_user}@{qsub_host}"
            if idx == 0:
                cmd_arr = ["ssh", host_str, "mkdir", "-p", qsub_tmp_dir]
                print(f"running command {' '.join(map(str, cmd_arr))}...")
                sh_result = subprocess.run(cmd_arr, capture_output=True)
                if sh_result.returncode != 0:
                    continue
                else:
                    print("success...")

            # copy aeroval config file to qsub host
            host_str = f"{qsub_user}@{qsub_host}:{qsub_tmp_dir}/"
            cmd_arr = [*REMOTE_CP_COMMAND, _file, host_str]
            print(f"running command {' '.join(map(str, cmd_arr))}...")
            sh_result = subprocess.run(cmd_arr, capture_output=True)
            if sh_result.returncode != 0:
                continue
            else:
                print("success...")
            # create qsub runfile and copy that to the qsub host
            qsub_run_file_name = _file.with_name(f"{_file.stem}{'.run'}")
            remote_qsub_run_file_name = Path.joinpath(qsub_tmp_dir, qsub_run_file_name.name)
            remote_json_file = Path.joinpath(qsub_tmp_dir, _file.name)
            dummy_str = get_runfile_str(
                remote_json_file,
                wd=qsub_tmp_dir,
                script_name=remote_qsub_run_file_name,
                hold_pattern=hold_pattern,
            )
            print(f"writing file {qsub_run_file_name}")
            with open(qsub_run_file_name, "w") as f:
                f.write(dummy_str)

            # copy runfile to qsub host
            host_str = f"{qsub_user}@{qsub_host}:{qsub_tmp_dir}/"
            cmd_arr = [*REMOTE_CP_COMMAND, qsub_run_file_name, host_str]
            print(f"running command {' '.join(map(str, cmd_arr))}...")
            sh_result = subprocess.run(cmd_arr, capture_output=True)
            if sh_result.returncode != 0:
                continue
            else:
                print("success...")

            # run qsub
            # unfortunatly qsub can't be run directly for some reason (likely security)
            # create a script with the qsub call and start that
            host_str = f"{qsub_user}@{qsub_host}:{qsub_tmp_dir}/"
            qsub_start_file_name = _file.with_name(f"{_file.stem}{'.sh'}")
            remote_qsub_run_file_name = Path.joinpath(qsub_tmp_dir, qsub_run_file_name.name)
            remote_qsub_start_file_name = Path.joinpath(qsub_tmp_dir, qsub_start_file_name.name)
            # this does not work:
            # cmd_arr = ["ssh", host_str, "/usr/bin/bash", "-l", "qsub", remote_qsub_run_file_name]
            # use bash script as workaround
            start_script_str = f"""#!/bin/bash -l
qsub {remote_qsub_run_file_name}

"""
            with open(qsub_start_file_name, "w") as f:
                f.write(start_script_str)
            cmd_arr = [*REMOTE_CP_COMMAND, qsub_start_file_name, host_str]
            if submit_flag:
                print(f"running command {' '.join(map(str, cmd_arr))}...")
                sh_result = subprocess.run(cmd_arr, capture_output=True)
                if sh_result.returncode != 0:
                    continue
                else:
                    print("success...")

                host_str = f"{qsub_user}@{qsub_host}"
                cmd_arr = [
                    "ssh",
                    host_str,
                    "/usr/bin/bash",
                    "-l",
                    remote_qsub_start_file_name,
                ]
                print(f"running command {' '.join(map(str, cmd_arr))}...")
                sh_result = subprocess.run(cmd_arr, capture_output=True)
                if sh_result.returncode != 0:
                    print(f"return code: {sh_result.returncode}")
                    print(f"{sh_result.stderr}")
                    continue
                else:
                    print("success...")
                    print(f"{sh_result.stdout}")

            else:
                print(f"qsub files created and copied to {qsub_host}.")
                print(
                    f"you can start the job with the command: qsub {remote_qsub_run_file_name} on the host {qsub_host}."
                )

        else:
            # localhost flag is set
            # scripts exist already, but in /tmp where the queue nodes can't read them
            # copy to submission directories
            # create tmp dir on qsub host; retain some parts
            if idx == 0:
                cmd_arr = ["mkdir", "-p", qsub_tmp_dir]
                print(f"running command {' '.join(map(str, cmd_arr))}...")
                sh_result = subprocess.run(cmd_arr, capture_output=True)
                if sh_result.returncode != 0:
                    continue
                else:
                    print("success...")

            # copy aeroval config file to cluster readable location
            host_str = f"{qsub_tmp_dir}/"
            cmd_arr = [*CP_COMMAND, _file, host_str]
            print(f"running command {' '.join(map(str, cmd_arr))}...")
            sh_result = subprocess.run(cmd_arr, capture_output=True)
            if sh_result.returncode != 0:
                continue
            else:
                print("success...")
            # create qsub runfile and copy that to cluster readable location
            qsub_run_file_name = _file.with_name(f"{_file.stem}{'.run'}")
            remote_qsub_run_file_name = Path.joinpath(qsub_tmp_dir, qsub_run_file_name.name)
            remote_json_file = Path.joinpath(qsub_tmp_dir, _file.name)
            dummy_str = get_runfile_str(
                remote_json_file,
                wd=qsub_tmp_dir,
                script_name=remote_qsub_run_file_name,
                hold_pattern=hold_pattern,
            )
            print(f"writing file {qsub_run_file_name}")
            with open(qsub_run_file_name, "w") as f:
                f.write(dummy_str)

            # copy runfile to cluster readable location
            host_str = f"{qsub_tmp_dir}/"
            cmd_arr = [*CP_COMMAND, qsub_run_file_name, host_str]
            print(f"running command {' '.join(map(str, cmd_arr))}...")
            sh_result = subprocess.run(cmd_arr, capture_output=True)
            if sh_result.returncode != 0:
                continue
            else:
                print("success...")

            # run qsub
            # unfortunatly qsub can't be run directly for some reason (likely security)
            # create a script with the qsub call and start that
            host_str = f"{qsub_tmp_dir}/"
            qsub_start_file_name = _file.with_name(f"{_file.stem}{'.sh'}")
            remote_qsub_run_file_name = Path.joinpath(qsub_tmp_dir, qsub_run_file_name.name)
            remote_qsub_start_file_name = Path.joinpath(qsub_tmp_dir, qsub_start_file_name.name)
            # this does not work:
            # cmd_arr = ["ssh", host_str, "/usr/bin/bash", "-l", "qsub", remote_qsub_run_file_name]
            # use bash script as workaround
            start_script_str = f"""#!/bin/bash -l
qsub {remote_qsub_run_file_name}

"""
            with open(qsub_start_file_name, "w") as f:
                f.write(start_script_str)
            cmd_arr = [*CP_COMMAND, qsub_start_file_name, host_str]
            if submit_flag:
                print(f"running command {' '.join(map(str, cmd_arr))}...")
                sh_result = subprocess.run(cmd_arr, capture_output=True)
                if sh_result.returncode != 0:
                    continue
                else:
                    print("success...")

                host_str = f"{qsub_user}@{qsub_host}"
                cmd_arr = ["/usr/bin/bash", "-l", remote_qsub_start_file_name]
                print(f"running command {' '.join(map(str, cmd_arr))}...")
                sh_result = subprocess.run(cmd_arr, capture_output=True)
                if sh_result.returncode != 0:
                    print(f"return code: {sh_result.returncode}")
                    print(f"{sh_result.stderr}")
                    continue
                else:
                    print("success...")
                    print(f"{sh_result.stdout}")

            else:
                print(f"qsub files created on localhost.")
                print(f"you can start the job with the command: qsub {remote_qsub_run_file_name}.")


def combine_output(options: dict):
    """combine the json files of the parallelised outputs to a single target directory (experiment)"""
    import shutil

    # create outdir
    try:
        # remove files first
        try:
            shutil.rmtree(options["outdir"])
        except FileNotFoundError:
            pass
        Path.mkdir(options["outdir"], exist_ok=False)
    except FileExistsError:
        pass

    for idx, combinedir in enumerate(sorted(options["files"])):
        # tmp dirs look like this: tmpggb7k02d.0001, tmpggb7k02d.0002
        # create common assembly directory
        # assemble the data
        # move the experiment to the target directory

        print(f"input dir: {combinedir}")
        # {experiment_name}/experiments.json: files are identical over one parallelisation run
        if idx == 0:
            # copy first directory to options['outdir']
            for dir_idx, dir in enumerate(Path(combinedir).iterdir()):
                # there should be just one directory. Use the 1st only anyway
                if dir_idx == 0:
                    exp_dir = [child for child in dir.iterdir() if Path.is_dir(child)][0]

                    shutil.copytree(dir, options["outdir"], dirs_exist_ok=True)
                    out_target_dir = Path.joinpath(options["outdir"], exp_dir.name)
                    # adjust config file name to cfg_<project_name>_<experiment_name>.json
                    cfg_file = out_target_dir.joinpath(
                        f"cfg_{exp_dir.parts[-2]}_{exp_dir.parts[-1]}.json"
                    )
                    if cfg_file.exists():
                        new_cfg_file = out_target_dir.joinpath(
                            f"cfg_{options['outdir'].parts[-1]}_{exp_dir.parts[-1]}.json"
                        )
                        cfg_file.rename(new_cfg_file)
                        # TODO: adjust some parts of the config file to the new project name
                        # read config file to restore the right order of variables and models in the visualisation
                        with open(new_cfg_file, "r") as inhandle:
                            aeroval_config = json.load(inhandle)
                else:
                    pass
                    # There's something wrong with the directory structure!
        else:
            # workdir: combinedir/<model_dir>
            # cfg_testing_IASI.json  contour  hm  map  menu.json  ranges.json  regions.json  scat  statistics.json  ts
            inpath = Path(combinedir).joinpath(*list(exp_dir.parts[-2:]))
            inpath_dir_len = len(inpath.parts)
            for file_idx, _file in enumerate(sorted(inpath.glob("**/*.*json"))):
                # determine if file is in inpath or below
                tmp = _file.parts[inpath_dir_len:]
                if len(tmp) == 1:
                    cmp_file = tmp[0]
                else:
                    cmp_file = Path.joinpath(Path(*list(tmp)))

                out_target_dir = Path.joinpath(options["outdir"], exp_dir.name)

                if match_file(cmp_file, MERGE_EXP_FILES_TO_EXCLUDE):
                    # skip some files for now
                    print(f"file {_file} excluded for now")
                    continue
                elif match_file(cmp_file, MERGE_EXP_CFG_FILES):
                    # special treatment for the experiment configuration
                    # file names need to be adjusted
                    cfg_file = inpath.joinpath(f"cfg_{inpath.parts[-2]}_{inpath.parts[-1]}.json")
                    outfile = out_target_dir.joinpath(
                        f"cfg_{options['outdir'].parts[-1]}_{inpath.parts[-1]}.json"
                    )
                    if outfile.exists():
                        # sould always fire since we handle the 1st directory above
                        infiles = [cfg_file, outfile]
                        print(f"writing combined json file {outfile}...")
                        t = Thread(target=combine_json_files, args=(infiles, outfile))
                        t.start()
                        # combine_json_files(infiles, outfile)
                        # TODO:
                        #  probably Adjust {
                        #   "proj_info": {
                        #     "proj_id": "testing"
                        #   },
                        # and
                        #   "path_manager": {
                        # "proj_id": "testing",
                        # "exp_id": "IASI",
                        # "json_basedir": "/home/jang/data/aeroval-local-web/data/tmpggb7k02d.0001",
                        # "coldata_basedir": "/home/jang/data/aeroval-local-web/coldata/tmpggb7k02d.0001"
                        # },
                        # (should be options['outdir'].parts[-1])
                    else:
                        # copy
                        pass

                else:
                    if match_file(cmp_file, MERGE_EXP_FILES_TO_COMBINE):
                        # combine files
                        outfile = out_target_dir.joinpath(cmp_file)
                        if outfile.exists():
                            infiles = [_file, outfile]
                            print(f"writing combined json file {outfile}...")
                            combine_json_files(infiles, outfile)
                        else:
                            # copy file
                            print(
                                f"non-existing outfile for merge. Copying {_file} to {outfile}..."
                            )
                            shutil.copy2(_file, outfile)
                    else:
                        # copy file
                        outfile = out_target_dir.joinpath(cmp_file)
                        print(f"copying {_file} to {outfile}...")
                        shutil.copy2(_file, outfile)

            # reorder menu.json according to config initial config file (
            # webdisp_opts['var_order_menu'] and webdisp_opts['model_order_menu']
            # the model order is unfortunately usually coming from the order in the config file
            # (model_cfg[<model_names>]

        pass


def combine_json_files(infiles: list[str], outfile: str):
    """small helper to ingest infile into outfile"""

    result = {}
    for infile in infiles:
        # the target file might not exist e.g. due to missing model data at station location
        try:
            with open(infile, "r") as inhandle:
                # result.update(json.load(inhandle))
                result = dict_merge(result, json.load(inhandle))
        except FileNotFoundError:
            result = json.load(inhandle)

    with open(outfile, "w", encoding="utf-8") as outhandle:
        json.dump(result, outhandle, ensure_ascii=False, indent=4)


def dict_merge(dct: dict | None, merge_dct: dict):
    """Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    :param dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: dct
    """
    if dct is None:
        dct = deepcopy(merge_dct)
    else:
        for k, v in merge_dct.items():
            if k in dct:
                if isinstance(dct[k], dict) and isinstance(merge_dct[k], dict):
                    dict_merge(dct[k], merge_dct[k])
                else:
                    dct[k] = deepcopy(merge_dct[k])
            else:
                dct[k] = deepcopy(merge_dct[k])

    return dct


def match_file(file: str, file_mask_array: str | list[str] = MERGE_EXP_FILES_TO_COMBINE) -> bool:
    """small helper that matches a filename against a list if wildcards"""
    if isinstance(file_mask_array, str):
        file_mask_array = [file_mask_array]

    ret_val = False
    for _file_mask in file_mask_array:
        if fnmatch(file, _file_mask):
            ret_val = True
            break
    return ret_val


def read_config_var(config_file: str, cfgvar: str) -> dict:
    """method to read the aeroval config file

    returns the config variable"""

    # read aeroval configuration file
    if fnmatch(config_file, "*.py"):
        foo = SourceFileLoader("bla", config_file).load_module()
        # the following line does unfortunately not work since a module is not subscriptable
        # CFG = foo[options["cfgvar"]]
        # use getattr instead
        cfg = deepcopy(getattr(foo, cfgvar))

    elif fnmatch(config_file, "*.json"):
        with open(config_file, "r") as inhandle:
            cfg = json.load(inhandle)
    else:
        msg = f"""Error: {config_file} has to be either a Python file or a json file.
exiting now..."""
        print(msg)
        sys.exit(1)
    return cfg


def get_config_info(
    config_file: str,
    cfgvar: str,
    cfg: dict = None,
) -> dict:
    """method to return the used observations and variables in formatted way

    returns a dict with the obs network name as key and the corresponding variables values"""

    if not cfg:
        cfg = read_config_var(config_file=config_file, cfgvar=cfgvar)

    var_config = {}
    for _obs_network in cfg["obs_cfg"]:
        try:
            if cfg["obs_cfg"][_obs_network]["is_superobs"]:
                continue
        except KeyError:
            pass

        var_config[cfg["obs_cfg"][_obs_network]["obs_id"]] = cfg["obs_cfg"][_obs_network][
            "obs_vars"
        ]

    return var_config


def adjust_menujson(
    menujson_file: str, cfg: dict = None, config_file: str = None, cfgvar: str = None
) -> None:
    """helper to adjust the menu.json file according to a given aeroval config file"""
    # load aeroval config file
    # load menu.json
    # adjust menu.json

    if cfg is None:
        cfg = read_config_var(config_file, cfgvar)
    try:
        with open(menujson_file, "r") as inhandle:
            menu_json_dict = json.load(inhandle)
    except:
        sys.exit(1)

    # adust variable oder 1st, then model order
    # variable order is in cfg['var_order_menu']
    # model order is the one from cfg['model_cfg']
    menu_json_out_dict = {}
    for _var in cfg["var_order_menu"]:
        # not all vars noted in cfg["var_order_menu"] are necessarily in the file
        try:
            menu_json_out_dict[_var] = deepcopy(menu_json_dict[_var])
        except KeyError:
            continue
        # now adjust the order of menu_json_out_dict[_var]["obs"][<obsnetwork>]['Column'|'Surface'].keys()
        obs_networks_present = menu_json_out_dict[_var]["obs"].keys()
        for obs_networks_present in menu_json_out_dict[_var]["obs"]:
            for obs_vert_type in menu_json_out_dict[_var]["obs"][obs_networks_present]:
                current_obs_order_dict = deepcopy(
                    menu_json_out_dict[_var]["obs"][obs_networks_present][obs_vert_type]
                )
                menu_json_out_dict[_var]["obs"][obs_networks_present][obs_vert_type] = {}
                for _model in cfg["model_cfg"]:
                    # not all the model necessaryly provide all variables
                    try:
                        menu_json_out_dict[_var]["obs"][obs_networks_present][obs_vert_type][
                            _model
                        ] = current_obs_order_dict[_model]
                    except KeyError:
                        pass

    with open(menujson_file, "w", encoding="utf-8") as outhandle:
        json.dump(menu_json_out_dict, outhandle, ensure_ascii=False, indent=4)
    print(f"updated {menujson_file}")


def adjust_heatmapfile(
    heatmap_files: list[str | Path],
    cfg: dict = None,
    config_file: str = None,
    cfgvar: str = None,
) -> None:
    """helper to adjust the heatmap files (files matching AEROVAL_HEATMAP_FILES_MASK)
    according to a given aeroval config file"""

    # load aeroval config file
    # load file
    # adjust adjust it

    if cfg is None:
        cfg = read_config_var(config_file, cfgvar)

    for heatmap_file in heatmap_files:
        try:
            with open(heatmap_file, "r") as inhandle:
                heatmap_dict = json.load(inhandle)
        except:
            msg = f"Error: {heatmap_file} not found! Skipping"
            continue

        # adust variable oder 1st, then model order
        # variable order is in cfg['var_order_menu']
        # model order is the one from cfg['model_cfg']
        heatmap_out_dict = {}
        for _var in cfg["var_order_menu"]:
            try:
                heatmap_out_dict[_var] = deepcopy(heatmap_dict[_var])

            except KeyError:
                continue
            # now adjust the order of heatmap_out_dict[_var][<obsnetwork>]['Column'|'Surface'].keys()
            for obs_networks_present in heatmap_out_dict[_var]:
                for obs_vert_type in heatmap_out_dict[_var][obs_networks_present]:
                    current_obs_order_dict = deepcopy(
                        heatmap_out_dict[_var][obs_networks_present][obs_vert_type]
                    )
                    heatmap_out_dict[_var][obs_networks_present][obs_vert_type] = {}

                    for _model in cfg["model_cfg"]:
                        # not all the model necessaryly provide all variables
                        try:
                            heatmap_out_dict[_var][obs_networks_present][obs_vert_type][
                                _model
                            ] = current_obs_order_dict[_model]
                        except KeyError:
                            pass

        with open(heatmap_file, "w", encoding="utf-8") as outhandle:
            json.dump(heatmap_out_dict, outhandle, ensure_ascii=False, indent=4)
        print(f"updated {heatmap_file}")


def adjust_hm_ts_file(
    ts_files: list[str | Path],
    cfg: dict = None,
    config_file: str = None,
    cfgvar: str = None,
) -> None:
    """helper to adjust the hm/ts/*.json files according to a given aeroval config file"""
    # load aeroval config file
    # load json file(s)
    # adjust model order

    if cfg is None:
        cfg = read_config_var(config_file, cfgvar)

    for _file in ts_files:
        try:
            with open(_file, "r") as inhandle:
                heatmap_dict = json.load(inhandle)
        except:
            msg = f"Error: {_file} not found! Skipping"
            continue

        # adust variable oder 1st, then model order
        # variable order is in cfg['var_order_menu']
        # model order is the one from cfg['model_cfg']
        heatmap_out_dict = {}
        for _var in heatmap_dict:
            try:
                heatmap_out_dict[_var] = deepcopy(heatmap_dict[_var])
            except KeyError:
                continue
            # now adjust the order of heatmap_out_dict[_var][<obsnetwork>]['Column'|'Surface'].keys()
            for obs_networks_present in heatmap_out_dict[_var]:
                for obs_vert_type in heatmap_out_dict[_var][obs_networks_present]:
                    current_obs_order_dict = deepcopy(
                        heatmap_out_dict[_var][obs_networks_present][obs_vert_type]
                    )
                    heatmap_out_dict[_var][obs_networks_present][obs_vert_type] = {}

                    for _model in cfg["model_cfg"]:
                        # not all the model necessaryly provide all variables
                        try:
                            heatmap_out_dict[_var][obs_networks_present][obs_vert_type][
                                _model
                            ] = current_obs_order_dict[_model]
                        except KeyError:
                            pass

        with open(_file, "w", encoding="utf-8") as outhandle:
            json.dump(heatmap_out_dict, outhandle, ensure_ascii=False, indent=4)
        print(f"updated {_file}")
