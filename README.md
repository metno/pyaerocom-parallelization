# pyaerocom-parallelization
project to run pyaerocom aeroval tasks in parallel on the Met Norway PPI infrastructure

## minimal documentation:


__run aeroval config file on qsub host and do not submit jobs to queue:__

    aeroval_parallelize --noqsub -l <cfg-file>

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
