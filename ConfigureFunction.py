#!/usr/bin/env python

import RunConfigurations
import os
import esutil


# get a default config object
def GetConfig(where):

    # arguments for configuring the run
    run = RunConfigurations.RunConfigurations.default

    #hide these from user
    run['command'] = 'popen' #['system', 'popen']
    run['DBload'] = 'cx_Oracle'  # ['cx_Oracle', 'sqlldr'] How to write to DB. 
    run['doDES'] = False  # Run sextractor without any Balrog galaxies over full images
    run['bands'] = ['g','r','i','z','Y'] # Bands you'll get measurement catalogs for
    run['dualdetection'] = [1,2,3]  # Use None not to use detection image. Otherwise the indices in the array of bands.


    # will get passed as command line arguments to balrog
    balrog = RunConfigurations.BalrogConfigurations.default

    # DB connection info
    db = RunConfigurations.DBInfo.default

    # what files to run balrog over
    tileinfo = esutil.io.read('spte-tiles.fits')
    tiles = tileinfo['tilename']


    if where=='BNL':
        import BNLCustomConfig as CustomConfig
    if where=='NERSC':
        import NERSCCustomConfig as CustomConfig
    run, balrog, db, tiles = CustomConfig.CustomConfig(run, balrog, db, tiles, where)

    #q = SubmitQueue(run)
    return run, balrog, db, tiles


def Generate_Job(run, where):
    filename = 'job-%s-%s' %(run['label'], run['joblabel'])
    descr = ''

    num = run['nodes'] * run['ppn']
    if where=='BNL':
        descr = descr + 'mode: bynode\n'
        descr = descr + 'N: %i\n' %(run['nodes'])
        descr = descr + 'hostfile: auto\n'
        descr = descr + 'job_name: %s' %(filename)
        #cmd = 'mpirun -npernode 1 -np %i -hostfile %%hostfile%% ./WrapBalrog.py %s' %(run['nodes'], where, run['ppn'])
        cmd = 'mpirun -npernode %i -np %i -hostfile %%hostfile%% ./AllMpi.py %s' %(run['ppn'], num, where)
        out = 'command: |\n   %s\n%s' %(cmd, descr)
    
    elif where=='NERSC':
        descr = descr + '#PBS -q %s\n' %(run['queue'])
        descr = descr + '#PBS -l nodes=%i:ppn=%i\n' %(run['nodes'], run['ppn'])
        descr = descr + '#PBS -l walltime=%s\n' %(run['walltime'])
        descr = descr + '#PBS -N %s\n' %(filename)
        descr = descr + '#PBS -e %s.$PBS_JOBID.err\n' %(filename)
        descr = descr + '#PBS -o %s.$PBS_JOBID.out\n\n' %(filename)
        #cmd = 'cd $PBS_O_WORKDIR\nmpirun -np %i ./WrapBalrog.py %s %i' %(run['nodes'], where, run['ppn'])
        cmd = '%s\ncd $PBS_O_WORKDIR\nmpirun -np %i ./AllMpi.py %s' %(run['module_setup'], num, where)
        out = '%s%s' %(descr, cmd)

    job = open(filename, 'w')
    job.write(out)
    return filename

