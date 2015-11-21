#!/usr/bin/env python

import RunConfigurations
import os
import esutil


# get a default config object
def GetConfig(where):

    # arguments for configuring the run
    run = RunConfigurations.RunConfigurations.default

    #hide these from user
    run['DBload'] = 'cx_Oracle'  # ['cx_Oracle', 'sqlldr'] How to write to DB. 
    run['doDES'] = False  # Run sextractor without any Balrog galaxies over full images
    run['bands'] = ['g','r','i','z','Y'] # Bands you'll get measurement catalogs for
    run['dualdetection'] = [1,2,3]  # Use None not to use detection image. Otherwise the indices in the array of bands.
    run['nodesize'] = 24 # Real only, 24 is the amount on edison (not relevant at BNL)
    run['intermediate-clean'] = True # Delete an iteration's output Balrog images
    run['tile-clean'] = True  # Delete the entire outdir/run's contents


    # will get passed as command line arguments to balrog
    balrog = RunConfigurations.BalrogConfigurations.default

    # DB connection info
    db = RunConfigurations.DBInfo.default

    # what files to run balrog over
    tileinfo = esutil.io.read('spte-tiles.fits')
    tiles = tileinfo['tilename']


    if where=='BNL':
        run['command'] = 'popen' #['system', 'popen']
        import BNLCustomConfig as CustomConfig
    if where=='NERSC':
        run['command'] = 'system' #['system', 'popen']
        import NERSCCustomConfig as CustomConfig
    run, balrog, db, tiles = CustomConfig.CustomConfig(run, balrog, db, tiles)

    #q = SubmitQueue(run)
    return run, balrog, db, tiles


def PBSadd(str, opt, val, start='#PBS'):
    str = str + '\n%s %s %s' %(start, opt, val)
    return str

def Generate_Job(run, where):
    filename = 'job-%s-%s' %(run['label'], run['joblabel'])
    descr = ''

    num = run['nodes'] * run['ppn']
    if where=='BNL':
        descr = descr + 'mode: bynode\n'
        descr = descr + 'N: %i\n' %(run['nodes'])
        descr = descr + 'hostfile: auto\n'
        descr = descr + 'job_name: %s' %(filename)
        cmd = 'mpirun -npernode %i -np %i -hostfile %%hostfile%% ./AllMpi.py %s' %(run['ppn'], num, where)
        out = 'command: |\n   %s\n%s' %(cmd, descr)
   
    elif where=='NERSC':
        descr = "#!/bin/bash"
        descr = PBSadd(descr, '-q', run['queue'])
        descr = PBSadd(descr, '-l', 'mppwidth=%i'%(run['nodesize']*run['nodes']))
        descr = PBSadd(descr, '-l', 'walltime=%s'%(run['walltime']))
        descr = PBSadd(descr, '-N', filename)
        descr = PBSadd(descr, '-j', 'oe')
        descr = PBSadd(descr, '-m', 'ae')

        descr = descr + '\n\ncd $PBS_O_WORKDIR'
        descr = descr + '\n%s' %(run['module_setup'])
        descr = descr + '\naprun -n %i -N %i' %(num, run['ppn'])

        if run['hyper-thread'] > 1:
            descr = descr + ' -j %i'%(run['hyper-thread'])
        descr = descr + ' ./AllMpi.py %s' %(where)

        out = descr
        filename = '%s.pbs' %(filename)

    job = open(filename, 'w')
    job.write(out)
    return filename

