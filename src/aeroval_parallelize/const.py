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

# from os import getppid

DEFAULT_CFG_VAR = "CFG"
RUN_UUID = uuid4()
RND = randint(0, 1e9)
# RND = getppid()
HOSTNAME = gethostname()
USER = getuser()
TMP_DIR = "/tmp"
# TMP_DIR = f"/home/{USER}/data/aeroval-local-web/data"

JSON_RUNSCRIPT_NAME = "aeroval_run_json_cfg"
# qsub binary
QSUB_NAME = "/usr/bin/qsub"
# qsub submission host
QSUB_HOST = "ppi-clogin-b1.met.no"
# directory, where the files will bew transferred before they are run
# Needs to be on Lustre or home since /tmp is not shared between machines
QSUB_DIR = f"/lustre/storeA/users/{USER}/submission_scripts"

# user name on the qsub host
QSUB_USER = USER
# queue name
QSUB_QUEUE_NAME = "research-el7.q"
# log directory
QSUB_LOG_DIR = "/lustre/storeA/project/aerocom/logs/aeroval_logs/"

# some copy constants
REMOTE_CP_COMMAND = ["scp", "-v"]
CP_COMMAND = ["cp", "-v"]

# Name of conda env to use for running the aeroval analysis
CONDA_ENV = "pya_para"
