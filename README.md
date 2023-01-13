# pyaerocom-parallelization
project to run pyaerocom aeroval tasks in parallel on the Met Norway PPI infrastructure

## general concept

- aeroval config files can be run in parallel by running all models and all observation networks
in parallel. For the observation networks that works unfortunately only if they are not combined
from other obs networks (aeroval limitation)
- aeroval config files can be located either on PPI or on local machines. In the latter case, 
all necessary files are copied to the GridEngine submit host and submitted from there
- The whole parallelisation happens in four steps:
  1. create cache files so that all parallel jobs have always a cache hit
  2. submit aeroval config file in parallel
  3. assemble the json files (after all jobs have finished)
  4. adjust variable and model order
  5. remove temporary data (omitted atm)
- runtime environment is defined via a conda environment in the `aerocom/conda2022/0.1.0` module 
(named `pya-para` by default; maintained by jang). Venvs are not supported at the moment.

### minimal documentation:

__Getting started:__

include the following lines in your `.bashrc` on your PPI home:

```bash
. /etc/os-release
if [ ${VERSION_ID} == '8.5' ]
then
  module use /modules/MET/rhel8/user-modules/
  # >>> conda initialize >>>
  # !! Contents within this block are managed by 'conda init' !!
  __conda_setup="$('/modules/rhel8/user-apps/aerocom/conda2022/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
  if [ $? -eq 0 ]; then
           eval "$__conda_setup"
  fi
  unset __conda_setup
  # <<< conda initialize <<<
fi
```
In case you still want to work with conda outside of the RHEL 8 machines, you can use the variable `VERSION_ID` to
distinguish between the OSes (`'7'` for CentOS7 and `'18.04'` for bionic)

you can activate the aeroval parallelization conda environment with the command `conda activate pya_para`

If everything went right, the command `aeroval_parallelize -h` should give you the following output:
```
usage: aeroval_parallelize [-h] [-v] [-e ENV] [--jsonrunscript JSONRUNSCRIPT] [--cfgvar CFGVAR] [--tempdir TEMPDIR]
                           [--remotetempdir REMOTETEMPDIR] [--json_basedir JSON_BASEDIR] [--coldata_basedir COLDATA_BASEDIR]
                           [--io_aux_file IO_AUX_FILE] [-l] [--nocache] [--queue QUEUE] [--cache_queue CACHE_QUEUE]
                           [--qsub-host QSUB_HOST] [--queue-user QUEUE_USER] [--noqsub] [--qsub-id QSUB_ID] [--qsub-dir QSUB_DIR]
                           [-o OUTDIR] [-c] [-a] [--adjustmenujson] [--adjustheatmap]
                           files [files ...]

command line interface to aeroval parallelisation.

positional arguments:
  files                 file(s) to read, directories to combine (if -c switch is used)

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         switch on verbosity
  -e ENV, --env ENV     conda env used to run the aeroval analysis; defaults to pya_para
  --jsonrunscript JSONRUNSCRIPT
                        script to run json config files; defaults to aeroval_run_json_cfg
  --cfgvar CFGVAR       variable that holds the aeroval config in the file(s) provided. Defaults to CFG
  --tempdir TEMPDIR     directory for temporary files; defaults to /tmp
  --remotetempdir REMOTETEMPDIR
                        directory for temporary files on qsub node; defaults to /tmp
  --json_basedir JSON_BASEDIR
                        set json_basedir in the config manually
  --coldata_basedir COLDATA_BASEDIR
                        set coldata_basedir in the configuration manually
  --io_aux_file IO_AUX_FILE
                        set io_aux_file in the configuration file manually
  -l, --localhost       start queue submission on localhost

caching options:
  options for cache file generation

  --nocache             switch off cache generation before running aeroval

queue options:
  options for running on PPI

  --queue QUEUE         queue name to submit the jobs to; defaults to research-r8.q
  --cache_queue CACHE_QUEUE
                        queue name to submit the caching jobs to; defaults to researchshort-r8.q
  --qsub-host QSUB_HOST
                        queue submission host; defaults to ppi-r8login-b1.int.met.no
  --queue-user QUEUE_USER
                        queue user; defaults to jang
  --noqsub              do not submit to queue (all files created and copied, but no submission)
  --qsub-id QSUB_ID     id under which the qsub commands will be run. Needed only for automation.
  --qsub-dir QSUB_DIR   directory under which the qsub scripts will be stored. defaults to
                        /lustre/storeB/users/jang/submission_scripts, needs to be on fs mounted by all queue hosts.

data assembly:
  options for assembly of parallisations output

  -o OUTDIR, --outdir OUTDIR
                        output directory for experiment assembly
  -c, --combinedirs     combine the output of a parallel runs; MUST INCLUDE <project dir>/<experiment dir>!!

adjust variable and model order:
  options to change existing order of variables and models

  -a, --adjustall       <aeroval cfgfile> <path to menu.json>; adjust order of all models/variables to aeroval config file
  --adjustmenujson      <aeroval cfgfile> <path to menu.json>; adjust order of menu.json to aeroval config file
  --adjustheatmap       <aeroval cfgfile> <path to glob_*_monthly.json>; adjust order of menu.json to aeroval config file

Example usages:

run script on qsub host and do not submit jobs to queue:
    aeroval_parallelize --noqsub -l <cfg-file>

run script on workstation, set directory of aeroval files and submit to queue via qsub host:
   aeroval_parallelize  --remotetempdir <directory for aeroval files> <cfg-file>
   
   Note that the directory for aeroval files needs to be on a common file system for all cluster machines.
   
set data directories and submit to queue:
    aeroval_parallelize --json_basedir /tmp/data --coldata_basedir /tmp/coldata --io_aux_file /tmp/gridded_io_aux.py <cfg-file>

assemble aeroval data after a parallel run has been finished: (runs always on the local machine)
     The output directory needs to be the target experiment's output path ! 
    aeroval_parallelize -c -o <output directory> <input directories>
    aeroval_parallelize -c -o ${HOME}/tmp/testing/IASI/ ${HOME}/tmpt39n2gp_*

adjust all variable and model orders to the one given in a aeroval config file:
    aeroval_parallelize --adjustall <aeroval-cfg-file> <path to menu.json>
    aeroval_parallelize --adjustall  /tmp/config/cfg_cams2-82_IFS_beta.py /tmp/data/testmerge_all/IFS-beta/menu.json

```

__Recommendation on the configuration file:__ It's recommended to use absolut paths in the config file. This makes sure 
the user and the system knows exactly where to put all files.


__run aeroval config file on qsub host and do not submit jobs to queue (testing):__

    aeroval_parallelize --noqsub -l <cfg-file>

__run aeroval config file on qsub host and submit jobs to queue:__

    aeroval_parallelize -l <cfg-file>

__run aeroval config on workstation, set directory of aeroval files and submit to queue via qsub host:__

    aeroval_parallelize --remotetempdir <directory for aeroval files> <cfg-file>
   
   Note that the directory for aeroval files needs to be on a common file system for all cluster machines.
   
__set data directories and submit to queue:__

    aeroval_parallelize --json_basedir /tmp/data --coldata_basedir /tmp/coldata --io_aux_file /tmp/gridded_io_aux.py <cfg-file>

__assemble aeroval data after a parallel run has been finished: (runs always on the local machine)__

    aeroval_parallelize -c -o <output directory> <input directories>
    aeroval_parallelize -c -o ${HOME}/tmp ${HOME}/tmpt39n2gp_*

__adjust all variable and model orders to the one given in a aeroval config file:__

    aeroval_parallelize --adjustall <aeroval-cfg-file> <path to menu.json>
    aeroval_parallelize --adjustall  /tmp/config/cfg_cams2-82_IFS_beta.py /tmp/data/testmerge_all/IFS-beta/menu.json

## cache file generation

Part of this project is also a simple cache file generator for pyaerocom. After a standard installation of this package, 
it is available as the command `pyaerocom_cachegen`

### minimal documentation

__start cache creation serially on localhost__

  ```
  pyaerocom_cachegen --vars concpm10 concpm25 -o EEAAQeRep.v2
  ```

__start cache creation parallel on qsub host (current host is NOT qsub host)__

  ```
  pyaerocom_cachegen --qsub --vars ang4487aer od550aer -o AeronetSunV3Lev2.daily
  ```

__start cache creation parallel on qsub host (current host IS qsub host)__

  ```
  pyaerocom_cachegen -l --qsub --vars concpm10 concpm25 vmro3 concno2 -o EEAAQeRep.NRT
  ```
