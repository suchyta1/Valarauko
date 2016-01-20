#!/usr/bin/env python

import imp
import os
import sys
import esutil
import json
import datetime

updir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
RunConfigurations = imp.load_source('RunConfigurations', os.path.join(updir,'RunConfigurations.py'))
#import RunConfigurations


# get a default config object
def GetConfig(where, config):

    # arguments for configuring the run
    run = RunConfigurations.RunConfigurations.default

    #hide these from user
    run['DBload'] = 'cx_Oracle'  # ['cx_Oracle', 'sqlldr'] How to write to DB. 
    run['bands'] = ['g','r','i','z','Y'] # Bands you'll get measurement catalogs for
    run['dualdetection'] = [1,2,3]  # Use None not to use detection image. Otherwise the indices in the array of bands.
    run['intermediate-clean'] = True # Delete an iteration's output Balrog images
    run['tile-clean'] = True  # Delete the entire outdir/run's contents
    run['queue'] = 'regular' # Probably no one other than Eric Suchyta will ever use the debug queue with this.


    run['balrog_as_function'] = True
    run['command'] = 'popen' #['system', 'popen']
    run['useshell'] = False # Only relevant with popen
    run['retry'] = True


    # will get passed as command line arguments to balrog
    balrog = RunConfigurations.BalrogConfigurations.default

    # DB connection info
    db = RunConfigurations.DBInfo.default

    # what files to run balrog over
    tileinfo = esutil.io.read(os.path.join(updir, 'tiles', 'spte-tiles.fits'))
    tiles = tileinfo['tilename']


    CustomConfig = imp.load_source('CustomConfig', config)
    run, balrog, db, tiles = CustomConfig.CustomConfig(run, balrog, db, tiles)
    balrog['systemcmd'] = run['command']
    balrog['retrycmd'] = run['retry']
    balrog['useshell'] = run['useshell']
        
    # This isn't supported in the new version. At least not yet, if ever.
    run['doDES'] = False  # Run sextractor without any Balrog galaxies over full images

    hours, minutes, seconds = run['walltime'].split(':')
    duration = datetime.timedelta(hours=float(hours), minutes=float(minutes), seconds=float(seconds))
    if where.upper()=='EDISON':
        if (run['queue']=='debug') and (duration.total_seconds() > 30*60):
            raise Exception("Walltime %s is too long for debug queue. Max is 00:30:00." %(run['walltime']))
        elif (run['queue']=='regular') and (run['nodes'] <= 682) and (duration.total_seconds() > 48.0*60.0*60.0):
            raise Exception("Walltime %s is too long for %i nodes in the regular queue. Max is 48:00:00." %(run['walltime'], run['nodes']))
        elif (run['queue']=='regular') and (run['nodes'] >= 683) and (run['nodes'] <= 4096) and (duration.total_seconds() > 36.0*60.0*60.0):
            raise Exception("Walltime %s is too long for %i nodes in the regular queue. Max is 36:00:00." %(run['walltime'], run['nodes']))
        elif (run['queue']=='regular') and (run['nodes'] >= 4097) and (duration.total_seconds() > 12.0*60.0*60.0):
            raise Exception("Walltime %s is too long for %i nodes in the regular queue. Max is 12:00:00." %(run['walltime'], run['nodes']))

    return run, balrog, db, tiles


def EndBNL(s):
    return '%s\n   '%(s)

def EndNERSC(s):
    return '%s\n'%(s)

def GetEnd(s, end):
    if end.upper()=='BNL':
        s = EndBNL(s)
    elif end.upper() in ['CORI','EDISON']:
        s = EndNERSC(s)
    return s


def BalrogDir(run, end):
    dir = os.path.dirname( os.path.realpath(run['balrog']) )
    d = "export PYTHONPATH=%s:${PYTHONPATH}"%(dir)
    d = GetEnd(d, end)
    return d

def Source(setup, end):
    s = ''
    if setup is not None:
        s = 'source %s' %(setup)
        s = GetEnd(s, end)
    return s


def SLURMadd(str, val, start='#SBATCH'):
    str = str + '\n%s %s' %(start, val)
    return str

def Generate_Job(run, where, jobname, dirname, jsonfile, setup):
    descr = ''
    thisdir = os.path.dirname(os.path.realpath(__file__))
    allmpi = os.path.join(thisdir, 'AllMpi.py')
    jobfile = os.path.join(dirname, jobname)
    logdir = os.path.join(dirname, 'runlog')

    s = Source(setup, where)
    d = BalrogDir(run, where)
    num = run['nodes'] * run['ppn']

    if where.upper()=='BNL':
        descr = descr + 'mode: bynode\n'
        descr = descr + 'N: %i\n' %(run['nodes'])
        descr = descr + 'hostfile: auto\n'
        descr = descr + 'job_name: %s' %(jobname)
        
        cmd = 'mpirun -npernode %i -np %i -hostfile %%hostfile%% %s %s %s' %(run['ppn'], num, allmpi, jsonfile, logdir)
        out = 'command: |\n   %s%s%s\n%s' %(s, d, cmd, descr)

        jobfile = '%s.wq' %(jobfile)
    

    elif where.upper() in ['CORI', 'EDISON']:
        descr = "#!/bin/bash -l \n"
        descr = SLURMadd(descr, '--partition=%s'%(run['queue']), start='#SBATCH')
        descr = SLURMadd(descr, '--nodes=%i'%(run['nodes']), start='#SBATCH')
        descr = SLURMadd(descr, '--time=%s'%(run['walltime']), start='#SBATCH')
        descr = SLURMadd(descr, '--job-name=%s'%(jobname), start='#SBATCH')
        descr = SLURMadd(descr, '--output=%s-%%j.out'%(jobname), start='#SBATCH')
        descr = SLURMadd(descr, '--mail-type=BEGIN,END,TIME_LIMIT_50', start='#SBATCH')
        descr = descr + '\n\n'

        descr =  descr + s + d
        descr = descr + 'srun -n %i %s %s %s' %(num, allmpi, jsonfile, logdir)

        out = descr
        jobfile = '%s.sl' %(jobfile)


    with open(jobfile, 'w') as job:
        job.write(out)

    return jobfile


def GetWhere(argv):
    setup = None
    where = argv[1]
    config = argv[2]
    dir = argv[3]
    if len(argv) > 4:
        setup = argv[4]

    return where, setup, config, dir


def GenJob(argv):
    where, setup, config, dir = GetWhere(argv)
    run, balrog, db, tiles = GetConfig(where, config)

    jobname = '%s-%s' %(run['label'], run['joblabel'])
    dirname = os.path.join(dir, '%s-jobdir' %(jobname))
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

    job = Generate_Job(run, where, jobname, dirname, jsonfile, setup)
    return job, where


if __name__ == "__main__":

    job, where = GenJob(sys.argv)
    print 'Wrote job file to:', job
