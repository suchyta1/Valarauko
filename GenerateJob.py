#!/usr/bin/env python

import RunConfigurations
import os
import sys
import esutil
import json
import datetime


# get a default config object
def GetConfig(where):

    # arguments for configuring the run
    run = RunConfigurations.RunConfigurations.default

    #hide these from user
    run['DBload'] = 'cx_Oracle'  # ['cx_Oracle', 'sqlldr'] How to write to DB. 
    run['doDES'] = False  # Run sextractor without any Balrog galaxies over full images
    run['bands'] = ['g','r','i','z','Y'] # Bands you'll get measurement catalogs for
    run['dualdetection'] = [1,2,3]  # Use None not to use detection image. Otherwise the indices in the array of bands.
    run['intermediate-clean'] = True # Delete an iteration's output Balrog images
    run['tile-clean'] = True  # Delete the entire outdir/run's contents

    run['queue'] = 'regular' # Probably no one other than Eric Suchyta will ever use the debug queue with this.
    run['nodesize'] = 24 # Real only, 24 is the amount on edison (not relevant at BNL)
    run['hyper-thread'] = 1 # 1 or 2 are the possibilities, 1 means don't hyperthread.

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
    elif where=='NERSC':
        run['command'] = 'system' #['system', 'popen']
        import NERSCCustomConfig as CustomConfig
    elif where=='CORI':
        run['command'] = 'system' #['system', 'popen']
        import NERSCCustomConfig as CustomConfig

    run, balrog, db, tiles = CustomConfig.CustomConfig(run, balrog, db, tiles)

    hours, minutes, seconds = run['walltime'].split(':')
    duration = datetime.timedelta(hours=float(hours), minutes=float(minutes), seconds=float(seconds))
    if where=='NERSC':
        if (run['queue']=='debug') and (duration.total_seconds() > 30*60):
            raise Exception("Walltime %s is too long for debug queue. Max is 00:30:00." %(run['walltime']))
        elif (run['queue']=='regular') and (run['nodes'] <= 682) and (duration.total_seconds() > 48.0*60.0*60.0):
            raise Exception("Walltime %s is too long for %i nodes in the regular queue. Max is 48:00:00." %(run['walltime'], run['nodes']))
        elif (run['queue']=='regular') and (run['nodes'] >= 683) and (run['nodes'] <= 4096) and (duration.total_seconds() > 36.0*60.0*60.0):
            raise Exception("Walltime %s is too long for %i nodes in the regular queue. Max is 36:00:00." %(run['walltime'], run['nodes']))
        elif (run['queue']=='regular') and (run['nodes'] >= 4097) and (duration.total_seconds() > 12.0*60.0*60.0):
            raise Exception("Walltime %s is too long for %i nodes in the regular queue. Max is 12:00:00." %(run['walltime'], run['nodes']))

    return run, balrog, db, tiles


def PBSadd(str, opt, val, start='#PBS'):
    str = str + '\n%s %s %s' %(start, opt, val)
    return str

def SLURMadd(str, val, start='#SBATCH'):
    str = str + '\n%s %s' %(start, val)
    return str

def Generate_Job(run, where, jobname, dirname, jsonfile):
    descr = ''
    thisdir = os.path.dirname(os.path.realpath(__file__))
    allmpi = os.path.join(thisdir, 'AllMpi.py')
    jobfile = os.path.join(dirname, jobname)
    logdir = os.path.join(dirname, 'runlog')

    num = run['nodes'] * run['ppn']
    if where=='BNL':
        descr = descr + 'mode: bynode\n'
        descr = descr + 'N: %i\n' %(run['nodes'])
        descr = descr + 'hostfile: auto\n'
        descr = descr + 'job_name: %s' %(jobname)
        #cmd = 'mpirun -npernode %i -np %i -hostfile %%hostfile%% ./AllMpi.py %s' %(run['ppn'], num, where)
        cmd = 'mpirun -npernode %i -np %i -hostfile %%hostfile%% %s %s %s' %(run['ppn'], num, allmpi, jsonfile, logdir)
        out = 'command: |\n   %s\n%s' %(cmd, descr)

        jobfile = '%s.wq' %(jobfile)
   
    elif where=='NERSC':
        descr = "#!/bin/bash"
        descr = PBSadd(descr, '-q', run['queue'])
        descr = PBSadd(descr, '-l', 'mppwidth=%i'%(run['nodesize']*run['nodes']))
        descr = PBSadd(descr, '-l', 'walltime=%s'%(run['walltime']))
        descr = PBSadd(descr, '-N', jobname)
        descr = PBSadd(descr, '-j', 'oe')
        descr = PBSadd(descr, '-m', 'ae')

        #descr = descr + '\n\ncd $PBS_O_WORKDIR'
        descr = descr + '\n\n%s' %(run['module_setup'])
        descr = descr + '\naprun -n %i -N %i' %(num, run['ppn'])

        if run['hyper-thread'] > 1:
            descr = descr + ' -j %i'%(run['hyper-thread'])
        #descr = descr + ' ./AllMpi.py %s' %(where)
        descr = descr + ' %s %s %s' %(allmpi, jsonfile, logdir)

        out = descr
        jobfile = '%s.pbs' %(jobfile)

    elif where=='CORI':
        descr = "#!/bin/bash -l \n"
        descr = SLURMadd(descr, '--partition=%s'%(run['queue']), start='#SBATCH')
        descr = SLURMadd(descr, '--nodes=%i'%(run['nodes']), start='#SBATCH')
        descr = SLURMadd(descr, '--time=%s'%s(run['walltime']), start='#SBATCH')
        descr = SLURMadd(descr, '--job-name=%s'%(jobname), start='#SBATCH')
        descr = SLURMadd(descr, '--output=%s-%%j'%(jobname), start='#SBATCH')

        descr = descr + '\n\n%s' %(run['module_setup'])
        descr = descr + '\nsrun -n %i' %(num)
        descr = descr + ' %s %s %s' %(allmpi, jsonfile, logdir)

        out = descr
        jobfile = '%s.pbs' %(jobfile)


    with open(jobfile, 'w') as job:
        job.write(out)

    return jobfile


def GetWhere(argv):
    if len(argv) > 1:
        where = argv[1]
    else:
        where = 'BNL'
    return where


def GenJob(argv):
    where = GetWhere(argv)
    run, balrog, db, tiles = GetConfig(where)

    jobname = '%s-%s' %(run['label'], run['joblabel'])
    dirname = os.path.join(os.path.dirname(os.path.realpath(__file__)), '%s-jobdir' %(jobname))
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    jsonfile = os.path.join(dirname, 'config.json')
    config = {}
    config['run'] = run
    config['balrog'] = balrog
    config['db'] = db
    config['tiles'] = list(tiles)
    configfile = os.path.join(dir, )
    with open(jsonfile, 'w') as outfile:
        json.dump(config, outfile)

    job = Generate_Job(run, where, jobname, dirname, jsonfile)
    return job, where


if __name__ == "__main__":

    job, where = GenJob(sys.argv)
    print job
