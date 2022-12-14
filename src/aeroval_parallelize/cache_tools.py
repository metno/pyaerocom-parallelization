#!/usr/bin/env python3
"""
cache file generator CLI for pyaerocom

for usage via the PPI queues
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime

# from getpass import getuser
from pathlib import Path
from tempfile import mkdtemp

from aeroval_parallelize.const import (
    CONDA_ENV,
    CP_COMMAND,
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

# script start time
START_TIME = datetime.now().strftime("%Y%m%d_%H%M%S")

# starting part of the qsub job name
QSUB_SCRIPT_START = f"pya_{RND}_caching_"


def write_script(
    filename: str | Path, var: str = "od550aer", obsnetwork: str = "AeronetSunV3Lev2.daily"
):
    import os
    import stat

    script_proto = f"""#!/usr/bin/env python3
    
from pyaerocom.io import ReadUngridded

def main():
    reader = ReadUngridded("{obsnetwork}")
    data = reader.read(vars_to_retrieve="{var}")

if __name__ == "__main__":
    main()
"""
    with open(filename, "w") as f:
        f.write(script_proto)

    # make executable
    st = os.stat(filename)
    os.chmod(filename, st.st_mode | stat.S_IEXEC)


def get_runfile_str_arr(
    file,
    queue_name=QSUB_QUEUE_NAME,
    script_name=None,
    # wd=QSUB_DIR,
    wd=None,
    mail=f"{QSUB_USER}@met.no",
    logdir=QSUB_LOG_DIR,
    date=START_TIME,
    conda_env="pya_para",
) -> str:
    """create list of strings with runfile for gridengine"""
    # create runfile

    if wd is None:
        wd = Path(file).parent

    if script_name is None:
        script_name = str(file.with_name(f"{file.stem}{'.run'}"))
    elif isinstance(script_name, Path):
        script_name = str(script_name)

    # $ -N pya_{rnd}_caching_{Path(file).stem}

    runfile_str = f"""#!/usr/bin/env bash -l
    
#$ -S /bin/bash
#$ -N {Path(file).stem}
#$ -q {queue_name}
#$ -pe shmem-1 1
#$ -wd {wd}
#$ -l h_rt=96:00:00
#$ -l s_rt=96:00:00
"""
    # $ -l h_vmem=40G
    if mail is not None:
        runfile_str += f"#$ -M {mail}\n"
    runfile_str += f"""#$ -m abe

#$ -l h_rss=30G,mem_free=30G
#$ -shell y
#$ -j y
#$ -o {logdir}/
#$ -e {logdir}/
logdir="{logdir}/"
date="{date}"
logfile="${{logdir}}/${{USER}}.${{date}}.${{JOB_NAME}}.${{JOB_ID}}_log.txt"
__conda_setup="$('/modules/rhel8/user-apps/aerocom/conda2022/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]
then eval "$__conda_setup"
else
  echo conda not working! exiting...
  exit 1
fi
echo "Got $NSLOTS slots for job $SGE_TASK_ID." >> ${{logfile}}
module use /modules/MET/rhel8/user-modules >> ${{logfile}} 2>&1
module add aerocom/conda2022/0.1.0 >> ${{logfile}} 2>&1
module list >> ${{logfile}} 2>&1
conda activate {conda_env} >> ${{logfile}} 2>&1
conda env list >> ${{logfile}} 2>&1
set -x
python --version >> ${{logfile}} 2>&1
pwd >> ${{logfile}} 2>&1
echo "starting {file} ..." >> ${{logfile}}
{file} >> ${{logfile}} 2>&1

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

    qsub_tmp_dir = Path.joinpath(Path(qsub_dir), f"qsub.{runfiles[0].parts[-2]}")

    try:
        rnd = options["qsub_id"]
    except KeyError:
        rnd = RND

    # localhost_flag = False
    # if "localhost" in qsub_host or platform.node() in qsub_host:
    #     localhost_flag = True

    for idx, _file in enumerate(runfiles):
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

            # copy python runfile to qsub host
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
            dummy_str = get_runfile_str_arr(
                remote_json_file,
                wd=qsub_tmp_dir,
                script_name=remote_qsub_run_file_name,
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
                cmd_arr = ["ssh", host_str, "/usr/bin/bash", "-l", remote_qsub_start_file_name]
                print(f"running command {' '.join(map(str, cmd_arr))}...")
                sh_result = subprocess.run(cmd_arr)
                if sh_result.returncode != 0:
                    print(f"qsub failed!")
                    continue
                else:
                    print("success...")

            else:
                print(f"qsub files created and copied to {qsub_host}.")
                print(
                    f"you can start the job with the command: qsub {remote_qsub_run_file_name} on the host {qsub_host}."
                )

        else:
            # localhost flag is set
            # scripts exist already, but in /tmp where the queue nodes can't read them
            # copy to submission directories
            if idx == 0:
                cmd_arr = ["mkdir", "-p", qsub_tmp_dir]
                print(f"running command {' '.join(map(str, cmd_arr))}...")
                sh_result = subprocess.run(cmd_arr, capture_output=True)
                if sh_result.returncode != 0:
                    continue
                else:
                    print("success...")

            # copy aeroval config file to qsub host
            host_str = f"{qsub_tmp_dir}/"
            cmd_arr = [*CP_COMMAND, _file, host_str]
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
            dummy_str = get_runfile_str_arr(
                remote_json_file,
                wd=qsub_tmp_dir,
                script_name=remote_qsub_run_file_name,
            )
            print(f"writing file {qsub_run_file_name}")
            with open(qsub_run_file_name, "w") as f:
                f.write(dummy_str)

            # copy runfile to qsub submission directory
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
            start_script_arr = []
            start_script_arr.append("#!/bin/bash -l")
            start_script_arr.append(f"qsub {remote_qsub_run_file_name}")
            start_script_arr.append("")
            with open(qsub_start_file_name, "w") as f:
                f.write("\n".join(start_script_arr))
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
