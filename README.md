# pyaerocom-parallelization
project to run pyaerocom aeroval tasks in parallel on the Met Norway PPI infrastructure

## Installation
Standard installation is done via pip:

```bash
python -m pip install 'git+https://github.com/metno/pyaerocom-parallelization.git'
```

For a different branch than main
```bash
python -m pip install 'git+https://github.com/metno/pyaerocom-parallelization.git@<branch name>'
```

## general concept

- aeroval config files need to be on PPI
- aeroval config files can be run in parallel by running all models in parallel.
- The whole parallelisation happens in five steps:
  1. create cache files so that all parallel jobs have always a cache hit (for non pyaro obs networks)
  2. submit aeroval config files in parallel (one job per model)
  3. assemble the json files (after all jobs have finished)
  4. adjust variable and model order (since aeroval actively uses the order in the json files)
  5. remove temporary data (omitted atm)
- As runtime environment the aerotools modules are supported. The standard module used is `/modules/MET/rhel8/user-modules/fou-kl/aerotools/aerotools.conda`
Please note that the used module needs to provide the command line interface of the parallelization (e.g. the 
`aeroval_parallelize` command). If the group provided modules are used, only those ending with `.conda` can be used
for the parallelization at the moment.

### minimal documentation:

__Getting started:__

- load one of the aerotools modules ending with `.conda`
at the time of this writing the following where available:
```markdown
aerotools.conda                    
pya-v2024.03.conda                 
pya-v2024.03.NorESM.conda          
pya-v2024.03.ratpm25pm10.conda 
```
In order for them to work, the entire path has to be given. In the case of the preinstalled modules that path is 
`/modules/MET/rhel8/user-modules/fou-kl/aerotools/`

If you want to use your own module, you have to make sure parallelization has been installed there as well. 

If everything went right, the command `aeroval_parallelize -h` should give you the following output:
```markdown
usage: aeroval_parallelize [-h] [-v] [-m MODULE] [--jsonrunscript JSONRUNSCRIPT] [--cfgvar CFGVAR]
                           [--tempdir TEMPDIR] [--json_basedir JSON_BASEDIR]
                           [--coldata_basedir COLDATA_BASEDIR] [--io_aux_file IO_AUX_FILE]
                           [--nocache] [--cachegen-only] [--queue QUEUE]
                           [--cache_queue CACHE_QUEUE] [--queue-user QUEUE_USER] [--dry-qsub]
                           [--qsub-id QSUB_ID] [--qsub-dir QSUB_DIR] [--cacheram CACHERAM]
                           [--anaram ANARAM] [--assemblyram ASSEMBLYRAM] [-o OUTDIR] [-c] [-a]
                           [--adjustmenujson] [--adjustheatmap]
                           files [files ...]

command line interface to aeroval parallelisation.

positional arguments:
  files                 file(s) to read, directories to combine (if -c switch is used)

options:
  -h, --help            show this help message and exit
  -v, --verbose         switch on verbosity
  -m MODULE, --module MODULE
                        environment module to use; defaults to /modules/MET/rhel8/user-modules/fou-
                        kl/aerotools/aerotools.conda
  --jsonrunscript JSONRUNSCRIPT
                        script to run json config files; defaults to aeroval_run_json_cfg
  --cfgvar CFGVAR       variable that holds the aeroval config in the file(s) provided. Defaults to
                        CFG
  --tempdir TEMPDIR     directory for temporary files; defaults to /tmp
  --json_basedir JSON_BASEDIR
                        set json_basedir in the config manually
  --coldata_basedir COLDATA_BASEDIR
                        set coldata_basedir in the configuration manually
  --io_aux_file IO_AUX_FILE
                        set io_aux_file in the configuration file manually

caching options:
  options for cache file generation

  --nocache             switch off cache generation before running aeroval
  --cachegen-only       run the cache file generation only

queue options:
  options for running on PPI

  --queue QUEUE         queue name to submit the jobs to; defaults to research-r8.q
  --cache_queue CACHE_QUEUE
                        queue name to submit the caching jobs to; defaults to research-r8.q
  --queue-user QUEUE_USER
                        queue user; defaults to jang
  --dry-qsub            do not submit to queue (all files created, but no submission)
  --qsub-id QSUB_ID     id under which the qsub commands will be run. Needed only for automation.
  --qsub-dir QSUB_DIR   directory under which the qsub scripts will be stored. defaults to
                        /lustre/storeB/users/jang/submission_scripts, needs to be on fs mounted by
                        all queue hosts.
  --cacheram CACHERAM   RAM usage [GB] for cache queue jobs (defaults to 30GB).
  --anaram ANARAM       RAM usage [GB] for analysis queue jobs (defaults to 30GB).
  --assemblyram ASSEMBLYRAM
                        RAM usage [GB] for assembly queue jobs (defaults to 10GB.

data assembly:
  options for assembly of parallelizations output

  -o OUTDIR, --outdir OUTDIR
                        output directory for experiment assembly
  -c, --combinedirs     combine the output of a parallel runs; MUST INCLUDE <project
                        dir>/<experiment dir>!!

adjust variable and model order:
  options to change existing order of variables and models

  -a, --adjustall       <aeroval cfgfile> <path to menu.json>; adjust order of all models/variables
                        to aeroval config file
  --adjustmenujson      <aeroval cfgfile> <path to menu.json>; adjust order of menu.json to aeroval
                        config file
  --adjustheatmap       <aeroval cfgfile> <path to glob_*_monthly.json>; adjust order of menu.json
                        to aeroval config file

__Example usages__:

submit jobs to queue; parameters as defaults:
    aeroval_parallelize <cfg-file>

do not submit jobs to queue (dry-run):
    aeroval_parallelize --dry-qsub <cfg-file>

submit jobs to queue; use special module:
    aeroval_parallelize -m /modules/MET/rhel8/user-modules/fou-kl/aerotools/pya-v2024.03.conda <cfg-file>

run just the cache file generation:
    aeroval_parallelize --cachegen-only <cfg-file>

set data directories and submit to queue:
    aeroval_parallelize --json_basedir /tmp/data --coldata_basedir /tmp/coldata --io_aux_file /tmp/gridded_io_aux.py <cfg-file>

assemble aeroval data after a parallel run has been finished:
    __The output directory needs to be the target experiment's output path !__ 
    aeroval_parallelize -c -o <output directory> <input directories>
    aeroval_parallelize -c -o ${HOME}/tmp/testing/IASI/ ${HOME}/tmpt39n2gp_*

adjust all variable and model orders to the one given in a aeroval config file:
    aeroval_parallelize --adjustall <aeroval-cfg-file> <path to menu.json>
    aeroval_parallelize --adjustall /tmp/config/cfg_cams2-82_IFS_beta.py /tmp/data/testmerge_all/IFS-beta/menu.json


```

__Recommendation on the configuration file:__ It's recommended to use absolut paths in the config file. This makes sure 
the user and the system knows exactly where to put all files.


__run aeroval config file and do not submit jobs to queue (testing):__

    aeroval_parallelize ---dry-qsub <cfg-file>

__run aeroval config file on default queue queue:__

    aeroval_parallelize <cfg-file>

__set data directories and submit to queue:__

    aeroval_parallelize --json_basedir /tmp/data --coldata_basedir /tmp/coldata --io_aux_file /tmp/gridded_io_aux.py <cfg-file>

__assemble aeroval data after a parallel run has been finished:__

    aeroval_parallelize -c -o <output directory> <input directories>
    aeroval_parallelize -c -o ${HOME}/tmp ${HOME}/tmpt39n2gp_*

__adjust all variable and model orders to the one given in a aeroval config file:__

    aeroval_parallelize --adjustall <aeroval-cfg-file> <path to menu.json>
    aeroval_parallelize --adjustall  /tmp/config/cfg_cams2-82_IFS_beta.py /tmp/data/testmerge_all/IFS-beta/menu.json

## cache file generation

Part of this project is also a simple cache file generator for pyaerocom. After a standard installation of this package, 
it is available as the command `pyaerocom_cachegen`

### minimal documentation

__help screen__
```markdown
usage: pyaerocom_cachegen [-h] [--vars VARS [VARS ...]] [-o OBSNETWORKS [OBSNETWORKS ...]] [-v] [--tempdir TEMPDIR]
                          [-m MODULE] [-p] [--queue QUEUE] [--queue-user QUEUE_USER] [--qsub] [--qsub-id QSUB_ID]
                          [--qsub-dir QSUB_DIR] [--dry-qsub] [-s SUBMISSION_DIR] [-r RAM]

command line interface to pyaerocom cache file generator pyaerocom_cachegen.

options:
  -h, --help            show this help message and exit
  --vars VARS [VARS ...]
                        variable name(s) to cache
  -o OBSNETWORKS [OBSNETWORKS ...], --obsnetworks OBSNETWORKS [OBSNETWORKS ...]
                        obs networks(s) names to cache
  -v, --verbose         switch on verbosity
  --tempdir TEMPDIR     directory for temporary files; defaults to /tmp
  -m MODULE, --module MODULE
                        environment module to use; defaults to /modules/MET/rhel8/user-modules/fou-
                        kl/aerotools/aerotools.conda
  -p, --printobsnetworks
                        just print the names of the supported obs network

queue options:
  options for running on PPI

  --queue QUEUE         queue name to submit the jobs to; defaults to research-r8.q
  --queue-user QUEUE_USER
                        queue user; defaults to jang
  --qsub                submit to queue using the qsub command
  --qsub-id QSUB_ID     id under which the qsub commands will be run. Needed only for automation.
  --qsub-dir QSUB_DIR   directory under which the qsub scripts will be stored. defaults to
                        /lustre/storeB/users/jang/submission_scripts, needs to be on fs mounted by all queue hosts.
  --dry-qsub            create all files for qsub, but do not submit to queue
  -s SUBMISSION_DIR, --submission-dir SUBMISSION_DIR
                        directory submission scripts
  -r RAM, --ram RAM     RAM usage [GB] for queue

__Example usages__:
start cache generation serially
pyaerocom_cachegen --vars concpm10 concpm25 -o EEAAQeRep.v2

dry run cache generation for queue job
pyaerocom_cachegen --dry-qsub --vars ang4487aer od550aer -o AeronetSunV3Lev2.daily

use special module at queue run (__full module path needed!__)
pyaerocom_cachegen -m /modules/MET/rhel8/user-modules/fou-kl/aerotools/pya-v2024.03 --vars ang4487aer od550aer -o AeronetSunV3Lev2.daily

start cache generation parallel on PPI queue
pyaerocom_cachegen --qsub --vars ang4487aer od550aer -o AeronetSunV3Lev2.daily
pyaerocom_cachegen --qsub --vars concpm10 concpm25 vmro3 concno2 -o EEAAQeRep.NRT

```


__start cache creation on default queue__

  ```
  pyaerocom_cachegen --qsub --vars concpm10 concpm25 vmro3 concno2 -o EEAAQeRep.NRT
  ```
__start cache creation serially__

  ```
  pyaerocom_cachegen --vars concpm10 concpm25 -o EEAAQeRep.v2
  ```
__create all files necessary for queue submission, but don't submit to queue (testing)__

  ```
  pyaerocom_cachegen --dry-qsub --vars ang4487aer od550aer -o AeronetSunV3Lev2.daily
  ```
