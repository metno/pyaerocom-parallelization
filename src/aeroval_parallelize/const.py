#!/usr/bin/env python3
"""
parallelisation for aeroval processing

some common constant definitions


"""
from __future__ import annotations

from getpass import getuser
from random import randint
from socket import gethostname
from uuid import uuid4
from os import environ

# from os import getppid

DEFAULT_CFG_VAR = "CFG"
RUN_UUID = uuid4()
RND = randint(int(0), int(1e9))
# RND = getppid()
HOSTNAME = gethostname()
USER = getuser()
TMP_DIR = "/tmp"

DEFAULT_STORE = "B"
STORE = environ.get("PPIROOM", DEFAULT_STORE)

JSON_RUNSCRIPT_NAME = "aeroval_run_json_cfg"
# qsub binary
# QSUB_NAME = "/usr/bin/qsub"
QSUB_NAME = "/opt/sge/bin/lx-amd64/qsub"
# qsub submission host
if STORE == "B":
    QSUB_HOST = "ppi-r8login-b1.int.met.no"
    QSUB_QUEUE_NAME = "research-r8.q,bigmem-r8.q,researchlong-r8.q"
    QSUB_SHORT_QUEUE_NAME = "researchshort-r8.q,research-r8.q,researchlong-r8.q,bigmem-r8.q"
else:
    # the last time storeB was down, only the bigmem-r8.q was available from storeA
    QSUB_HOST = "ppi-r8login-a1.int.met.no"
    QSUB_QUEUE_NAME = "bigmem-r8.q"
    QSUB_SHORT_QUEUE_NAME = "bigmem-r8.q"

# directory, where the files will bew transferred before they are run
# Needs to be on Lustre or home since /tmp is not shared between machines
# QSUB_DIR = f"/lustre/storeA/users/{USER}/submission_scripts"
QSUB_DIR = f"/lustre/store{STORE}/users/{USER}/submission_scripts"

# user name on the qsub host
QSUB_USER = USER
# queue name
# log directory
QSUB_LOG_DIR = f"/lustre/store{STORE}/project/aerocom/logs/aeroval_logs/"

# some copy constants
REMOTE_CP_COMMAND = ["scp", "-v"]
CP_COMMAND = ["cp", "-v"]

# Name of conda env to use for running the aeroval analysis
CONDA_ENV = "pya_para"

# Name of default environment module
# ENV_MODULE_NAME = "/modules/MET/rhel8/user-modules/fou-kl/aerotools/aerotools"
ENV_MODULE_NAME = "/modules/MET/rhel8/user-modules/fou-kl/aerotools/aerotools.conda"

# default RAM asked for caching jobs (in GB)
DEFAULT_CACHE_RAM = 40

# default RAM for analysis jobs (in GB)
DEFAULT_ANA_RAM = 40
# default RAM for assembly jobs (in GB)
DEFAULT_ASSEMBLY_RAM = 10

# default module name
# DEFAULT_MODULE_NAME = "/modules/MET/rhel8/user-modules/fou-kl/aerotools/aerotools"

# depending on which module we use, we might need to change the python interpreter
# therefore make it easy to change that
DEFAULT_PYTHON = "python"

JSON_EXT = ".json"
PICKLE_JSON_EXT = ".picklejson"
