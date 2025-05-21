#!/usr/bin/env python3
"""
cache file generator CLI for pyaerocom

for usage via the PPI queues
"""
from __future__ import annotations

import subprocess
from datetime import datetime

from pathlib import Path

from aeroval_parallelize.const import (
    CONDA_ENV,
    CP_COMMAND,
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
    CONDA_ENV,
    # DEFAULT_MODULE_NAME,
    ENV_MODULE_NAME,
    DEFAULT_PYTHON,
)

# script start time
START_TIME = datetime.now().strftime("%Y%m%d_%H%M%S")

# starting part of the qsub job name
QSUB_SCRIPT_START = f"pya_{RND}_caching_"


def write_script_pyaro(
    filename: str | Path,
    conffile: str | Path = None,
    var: str = "od550aer",
    obsnetwork: str = "AeronetSunV3Lev2.daily",
    use_module: bool = False,
):
    """1st version for run with pyaro"""

    import os
    import stat

    if use_module:
        shebang = f"#!/usr/bin/env {DEFAULT_PYTHON}"
    else:
        shebang = "#!/usr/bin/env python"

    script_proto = f"""{shebang}
    
from pyaerocom.io import ReadUngridded
import jsonpickle

def main():
    with open("{conffile}", "r") as f:
        json_str = f.read()
        obsconf = jsonpickle.decode(json_str)
    reader = ReadUngridded("{obsnetwork}")
    data = reader.read(vars_to_retrieve="{var}", configs=obsconf)

if __name__ == "__main__":
    main()
"""
    with open(filename, "w") as f:
        f.write(script_proto)

    # make executable
    st = os.stat(filename)
    os.chmod(filename, st.st_mode | stat.S_IEXEC)


def write_script(
    filename: str | Path,
    var: str = "od550aer",
    obsnetwork: str = "AeronetSunV3Lev2.daily",
    use_module: bool = False,
):
    """version for run internal obs networks"""
    import os
    import stat

    if use_module:
        shebang = f"#!/usr/bin/env {DEFAULT_PYTHON}"
    else:
        shebang = "#!/usr/bin/env python"

    script_proto = f"""{shebang}
    
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


def get_runfile_str_arr_module(
    file,
    queue_name=QSUB_QUEUE_NAME,
    script_name=None,
    # wd=QSUB_DIR,
    wd=None,
    mail=f"{QSUB_USER}@met.no",
    logdir=QSUB_LOG_DIR,
    date=START_TIME,
    module=ENV_MODULE_NAME,
    ram=DEFAULT_CACHE_RAM,
) -> str:
    """create list of strings with runfile for gridengine using the aerotools modules @ PPI"""
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
#$ -l h_rt=4:00:00
#$ -l s_rt=4:00:00
"""
    # $ -l h_vmem=40G
    if mail is not None:
        runfile_str += f"#$ -M {mail}\n"
    runfile_str += f"""#$ -m abe

#$ -l h_rss={ram}G,mem_free={ram}G,h_data={ram}G
#$ -shell y
#$ -j y
#$ -o {logdir}/
#$ -e {logdir}/
logdir="{logdir}/"
date="{date}"
logfile="${{logdir}}/${{USER}}.${{date}}.${{JOB_NAME}}.${{JOB_ID}}_log.txt"
export PYAEROCOM_LOG_FILE="${{logdir}}/${{USER}}.${{date}}.${{JOB_NAME}}.${{JOB_ID}}_log.txt"

echo "Got $NSLOTS slots for job $SGE_TASK_ID." >> ${{logfile}}
module load {module} >> ${{logfile}} 2>&1
set -x
pya_python --version >> ${{logfile}} 2>&1
pwd >> ${{logfile}} 2>&1
echo "starting pya_python {file} ..." >> ${{logfile}}
pya_python {file} 

"""
    return runfile_str


def run_queue(
    conffiles: list[Path],
    qsub_host: str = QSUB_HOST,
    qsub_cmd: str = QSUB_NAME,
    qsub_dir: str = QSUB_DIR,
    qsub_user: str = QSUB_USER,
    qsub_queue: str = QSUB_QUEUE_NAME,
    submit_flag: bool = False,
    options: dict = {},
):
    """submit config files to the remote cluster

    # create submission file (create locally, copy to qsub host (fabric)

    """

    for idx, _file in enumerate(conffiles):
        # create qsub runfile
        qsub_run_file_name = _file.with_name(f"{_file.stem}{'.run'}")
        dummy_str = get_runfile_str_arr_module(
            _file,
            wd=_file.parent,
            script_name=qsub_run_file_name,
            queue_name=qsub_queue,
            module=options["env_mod"],
            ram=options["qsub_ram"],
        )

        with open(qsub_run_file_name, "w") as f:
            f.write(dummy_str)
        print(f"Wrote {qsub_run_file_name}")

        qsub_start_file_name = _file.with_name(f"{_file.stem}{'.sh'}")

        start_script_arr = []
        start_script_arr.append("#!/bin/bash -l")
        start_script_arr.append(f"qsub {qsub_run_file_name}")
        start_script_arr.append("")
        with open(qsub_start_file_name, "w") as f:
            f.write("\n".join(start_script_arr))
        print(f"Wrote {qsub_start_file_name}")
        if submit_flag:
            cmd_arr = ["/usr/bin/bash", "-l", qsub_start_file_name]
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
            print(f"qsub files created.")
            print(f"you can start the job with the command: qsub {qsub_run_file_name}.")
