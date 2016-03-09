#!/usr/bin/env python

import imp
import os
import sys
import esutil
import json
import datetime
import numpy as np
import copy

thisdir = os.path.dirname(os.path.realpath(__file__))
updir = os.path.dirname(thisdir)
RunConfigurations = imp.load_source('RunConfigurations', os.path.join(thisdir,'RunConfigurations.py'))
shiftermodule = imp.load_source('shifter', os.path.join(thisdir,'shifter.py'))
runtile = imp.load_source('runtile', os.path.join(thisdir,'RunTileJob.py'))

def Exit(msg):
    print msg
    sys.exit(1)

def TryToMake(dir):
    if not os.path.exists(dir):
        try:
            os.makedirs(dir)
        except:
            Exit( 'Could not make %s'%(dir) )


def NewOutdir(run, shifter, key='outdir'):
    dir = run[key].rstrip('/')
    out = dir.replace( os.path.dirname(dir), shifter.outroot )
    return out


# get a default config object
def GetConfig(where, config):

    # arguments for configuring the run
    run = RunConfigurations.RunConfigurations.default


    # hide these from user
    run['ppn'] = None
    run['downsample'] = None
    run['paralleldownload'] = True
    run['DBoverwrite'] =  False  # Overwrite DB tables with same names (if they exist). False means append into existing tables. Regardless, the tables will be created if they don't exist.
    run['duplicate'] = None
    run['allfail'] = True
    run['npersubjob'] = 1

    run['cores'] = None
    run['asdependency'] = False
    run['email'] = None # Set this to your email if running at BNL to get an email at finish. At NERSC, I've set slurm to send start, 50%, and finish emails automatically
    run['shifter'] = None
    run['stripe'] = None

    if where=='slurm':
        run['queue'] = 'regular' # Which queue to use if running at NERSC. 
        run['stripe'] = 2


    # will get passed as command line arguments to balrog
    balrog = RunConfigurations.BalrogConfigurations.default
    balrog['slrdir'] = None
    balrog['catalog'] = None

    # DB connection info
    db = RunConfigurations.DBInfo.default

    # what files to run balrog over
    tileinfo = esutil.io.read(os.path.join(updir, 'tiles', 'spte-tiles.fits'))
    tiles = tileinfo['tilename']


    CustomConfig = imp.load_source('CustomConfig', config)
    run, balrog, db, tiles = CustomConfig.CustomConfig(run, balrog, db, tiles)

    # This isn't supported in the new version. At least not yet, if ever.
    run['doDES'] = False  # Run sextractor without any Balrog galaxies over full images

    # These are supported, but I don't want them to be changed, unless you understand what the consequences mean. Having them here they're not editable in the config file.
    run['bands'] = ['g','r','i','z','Y'] # Bands you'll get measurement catalogs for. I haven't tested changing this -- don't.
    run['dualdetection'] = [1,2,3]  # Use None not to use detection image. Otherwise the indices in the array of bands. I haven't tested changing this -- don't.
    run['intermediate-clean'] = True # Delete an iteration's output Balrog images
    run['tile-clean'] = True  # Delete the entire outdir/run's contents
    run['balrog_as_function'] = True
    run['command'] = 'popen' #['system', 'popen']
    run['useshell'] = False # Only relevant with popen
    run['retry'] = False
    run['wgetmax'] = 10
    run['funpackmax'] = 10

    balrog['systemcmd'] = run['command']
    balrog['retrycmd'] = run['retry']
    balrog['useshell'] = run['useshell']
  
    if balrog['slrdir'] is None:
        Exit( "You need to set the slr directory (where the SLR FITS files live), because I can't make this publicly available" )
    if balrog['catalog'] is None:
        Exit( "You need to set the catalog to sample from." )

    if (run['shifter'] is not None):
        print "You're using shifter, super cool. I've detected shifter=%s. I'm configuring a bunch of stuff for you automatically that I don't let you overwrite."%(run['shifter'])
        shifter = shiftermodule.GetShifter(run,balrog)
    else:
        shifter = None

    mod = len(tiles) % (run['nodes'] * run['npersubjob'])
    if (mod != 0):
        Exit( "I only allow you to run a evenly divisible jobs (meaning len(tiles)/(nodes*npersubjob)=integer) in these wrappers because that's easier to understand, and doesn't really let you do any less." )
    run['ndependencies'] = len(tiles) / (run['nodes'] * run['npersubjob'])

    if where=='wq':
        if (run['ndependencies'] != 1):
            Exit( "With wq, I require len(tiles)/(nodes*npersubjob)=1. There's basically no reason not to run like this." )
        if run['asdependency']:
            Exit( "You cannot do job dependencies with wq. Must use asdependency=False." )

    if where=='slurm':
        if (not run['asdependency']) and (run['ndependencies']!=1):
            Exit( "With asdependency=False, I require len(tiles)/(nodes*npersubjob)=1." )

    if run['jobdir'] is None:
        run['jobdir'] = os.path.dirname(os.path.realpath(__file__))
        print 'No run jobdir given, setting it to: %s'%(run['jobdir'])
    TryToMake(run['jobdir'])
    if run['outdir'] is None:
        run['outdir'] = os.path.dirname(os.path.realpath(__file__))
        print 'No run jobdir given, setting it to: %s'%(run['outdir'])
    TryToMake(run['outdir'])


    run['jobname'] = '%s-%s' %(run['dbname'], run['joblabel'])
    run['jobdir'] = os.path.join(run['jobdir'], '%s-jobdir' %(run['jobname']))
    run['outdir'] = os.path.join(run['outdir'], run['jobname'])
    TryToMake(run['jobdir'])
    TryToMake(run['outdir'])
        

    """
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
    """

    return run, balrog, db, tiles, shifter



def WriteJson(config,dirname, tiles,start,end):
    jsonfile = os.path.join(dirname, '%s'%(runtile.Files.json))
    config['tiles'] = list(tiles[start:end])
    with open(jsonfile, 'w') as outfile:
        json.dump(config, outfile)
    return jsonfile


def StartJsonDir(run, dirname, id):
    jdir = runtile.GetJsonDir(run, dirname, id)
    if (not os.path.exists(jdir)):
        os.makedirs(jdir)
    return jdir


def SubConfig(start,i, tiles, run,config, jobdir, shifter=None):
    runcopy = copy.copy(run)
    end = start + run['npersubjob']
    id = i + 1

    sdir = StartJsonDir(run, jobdir, id)
    run['exitfile'] = os.path.join(sdir, runtile.Files.exit)
    run['startupfile'] = os.path.join(sdir, runtile.Files.startupfile)
    runcopy = copy.copy(run)

    if shifter is not None:
        runcopy['outdir'] = NewOutdir(runcopy, shifter)
        depdir = shifter.jobroot
        subdir = runtile.GetJsonDir(run, depdir, id)
        runcopy['pos'] = shifter.posroot
        #runcopy['slr'] = shifter.slrroot
        runcopy['exitfile'] = os.path.join(subdir, runtile.Files.exit)
        runcopy['startupfile'] = os.path.join(subdir, runtile.Files.startupfile)
    else:
        depdir = jobdir
        subdir = sdir

    runcopy['touchfile'] = os.path.join(depdir, runtile.Files.cok)
    runcopy['failfile'] = os.path.join(depdir, runtile.Files.cfail)
    runcopy['runlogdir'] = os.path.join(subdir, runtile.Files.runlog)
    runcopy['dupokfile'] = os.path.join(subdir, runtile.Files.dupok)
    runcopy['dupfailfile'] = os.path.join(subdir, runtile.Files.dupfail)
    runcopy['anyfail'] = os.path.join(subdir, runtile.Files.anyfail)
    
    if i==0:
        runcopy['isfirst'] = True
    else:
        runcopy['isfirst'] = False
    config['run'] = runcopy
    jsonfile = WriteJson(config, sdir, tiles,start,end)

    if shifter is not None:
        jsonfile = os.path.join(subdir, 'config.json')

    return jsonfile, end


def WriteOut(jobfile, out):
    with open(jobfile, 'w') as job:
        job.write(out)


def GetDepJobDir(run, jobname, k=0):
    if k > 0:
        run['DBoverwrite'] = False
    jobdir = run['jobdir']
    if run['ndependencies'] > 1:
        run['jobname'] = '%s_%s_%i'%(jobname,runtile.Files.depstr,k+1)
        jobdir = os.path.join(jobdir, '%s_%i'%(runtile.Files.depstr,k+1))
        TryToMake(jobdir)
    return jobdir


class CmdFormat(object):
    def __init__(self, indent='', cmd=''):
        self.indent = indent
        self.cmd = cmd

    def __iadd__(self, other):
        self.cmd = self.cmd + self.indent + other + '\n'
        return self

    def __add__(self, other):
        self.cmd = self.cmd + '\n' + other.cmd
        return self.cmd


def GetMainWork(setup, run, tiles, config, jobdir, shifter, space='', q='wq', scmds='', start=0):
    cmd = CmdFormat(indent=space)


    if shifter is None:
        if setup is not None:
            cmd += 'source %s' %(setup)
        dir = os.path.dirname( os.path.realpath(run['balrog']) )
        cmd += "export PYTHONPATH=%s:${PYTHONPATH}"%(dir)
    else:
        cmd += 'module load shifter'

    if run['stripe'] is not None:
        cmd += 'if ! [ -d %s ]; then mkdir %s; fi;' %(run['outdir'],run['outdir'])
        cmd += 'lfs setstripe %s --count %i' %(run['outdir'],run['stripe'])


    for i in range(run['nodes']):
        jsonfile, start = SubConfig(start,i, tiles, run,config, jobdir, shifter=shifter)
        jdir = os.path.basename( os.path.dirname(jsonfile) )

    thisdir = os.path.dirname(os.path.realpath(__file__))
    if shifter is not None:
        thisdir = shifter.thisdir
    allmpi = os.path.join(thisdir, 'RunTileJob.py')
    sendmail = os.path.join(thisdir, 'SendEmail.py')

    corestr = ''
    if run['cores'] is not None:
        corestr = '-c %i '%(run['cores'])

    if cmd.cmd.strip()!='':
        cmd += ''
    cmd += """dirindex=($(seq 1 %i))"""%(run['nodes'])
    if q=='wq':
        cmd += """nodes=(); while read -r line; do found=false; host=$line; for h in "${nodes[@]}"; do if [ "$h" = "$host" ]; then found=true; fi; done; if [ "$found" = "false" ]; then nodes+=("$host"); fi; done < %hostfile%"""

    cmd += """jobdir=%s"""%(jobdir)
    jj = 'jobdir'
    if shifter is not None:
        cmd += """sjobdir=%s"""%(shifter.jobroot)
        jj = 'sjobdir'

    file = "$jobdir/%s_$i/%s" %(runtile.Files.substr,runtile.Files.startupfile)
    cmd += """for i in ${dirindex[@]}; do if [ -f %s ]; then rm %s; fi; done"""%(file,file)
    
    if q=='wq':
        file = "$%s/%s_${dirindex[$i]}/%s" %(jj,runtile.Files.substr,runtile.Files.json)
        cmd += """for ((i=0;i<%i;i++)); do mpirun -np 1 -host ${nodes[$i]} %s%s %s & done"""%(run['nodes'],scmds,allmpi,file)
    elif q=='slurm':
        run['email'] = None
        file = "$%s/%s_$i/%s" %(jj,runtile.Files.substr,runtile.Files.json)
        if shifter is not None:
            cmd += """for i in ${dirindex[@]}; do srun -N 1 -n 1 %s%s /bin/bash -c "source /home/user/.bashrc; %s %s" & done""" %(corestr, scmds, allmpi, file)
        else:
            cmd += """for i in ${dirindex[@]}; do srun -N 1 -n 1 %s%s %s & done""" %(corestr, allmpi, file)

    cmd += 'wait\n'
    file = "$jobdir/%s_$i/%s" %(runtile.Files.substr,runtile.Files.exit)
    cmd += """fails=0; files=""; for i in ${dirindex[@]}; do read -r result < %s; if [ "$result" = "1" ]; then let "fails+=1"; if [ $fails = "1" ]; then files="%s"; else files="${files},%s"; fi; fi; done;"""%(file,file,file)
    cmd += """if [ $fails = "0" ]; then echo "job succeeded"; code=0; else echo "job failed -- $fails failures -- bad exit files: $files"; code=1; fi"""

    if run['email'] is not None:
        cmd += '%s %s %s $code'%(sendmail, run['email'], run['jobname'])

    cmd += 'exit $code'
    return cmd, start


def ShifterCmdline(img, jobdir, run, shifter, balrog):
    netrc = os.path.join(os.environ['HOME'])
    vols = [ [jobdir,shifter.jobroot], [run['outdir'],shifter.outroot], [netrc,shifter.homeroot], [balrog['slrdir'],shifter.slrroot], [run['pos'],shifter.posroot], [os.path.dirname(balrog['catalog']),shifter.catroot] ]
    scmds = 'shifter %s'%(img)
    for vol in vols:
        scmds = "%s --volume=%s:%s"%(scmds, vol[0],vol[1]) 
    return scmds

def SlurmDirectives(run, config, allnodes, jobdir, shifter, scmds=''):
    ofile = os.path.join(jobdir, '%s-%%j.out'%(run['jobname']))
    descr = CmdFormat(indent='#SBATCH ', cmd="#!/bin/bash -l \n\n")
    descr += '--job-name=%s'%(run['jobname'])
    descr += '--mail-type=BEGIN,END,TIME_LIMIT_50'
    descr += '--partition=%s'%(run['queue'])
    descr += '--time=%s'%(run['walltime'])
    descr += '--nodes=%i'%(allnodes)
    descr += '--output=%s'%(ofile)
    if run['shifter'] is not None:
        img = '--image=docker:%s'%(run['shifter'])
        descr += img
        scmds = ShifterCmdline(img, jobdir, run, shifter, config['balrog'])
        config['balrog']['slrdir'] = shifter.slrroot
        config['balrog']['catalog'] = os.path.join(shifter.catroot, os.path.basename(config['balrog']['catalog']))
    return descr, scmds


class DepWrite(object):
    def __init__(self, t, out):
        self.t = t
        self.out = out

    def write(self, cmd, level=0):
        t = ''
        for i in range(level):
            t = t + self.t
        self.out.write(t + cmd + '\n')


def WriteDepsJob(run, jobname,  t='    '):
    submit = os.path.join(run['jobdir'], 'submit.sh')
    with open(submit, 'w') as out:
        writer = DepWrite(t, out)
        name = '%s_%s_$i'%(jobname,runtile.Files.depstr)
        file = os.path.join('$jobdir', '%s_$i'%(runtile.Files.depstr), '%s.sl'%(name))

        writer.write('jobdir=%s'%(run['jobdir']), level=0)
        writer.write('arr=($(seq 1 %s))'%(run['ndependencies']), level=0)
        writer.write('for i in "${arr[@]}"; do', level=0)
        writer.write('if [ "$i" = "1" ]; then', level=1)
        writer.write('cmd="sbatch %s"'%(file), level=2)
        writer.write('else', level=1)
        writer.write('cmd="sbatch --dependency=afterok:$dep %s"'%(file), level=2)
        writer.write('fi', level=1)
        writer.write('out="$($cmd)"', level=1)
        writer.write('outarr=($out)', level=1)
        writer.write('dep=${outarr[${#outarr[@]}-1]}', level=1)
        writer.write('done', level=0)

    os.chmod(submit, 0755)
    jobfile = submit
    return jobfile


def wqDirectives(run):
    descr = CmdFormat(indent='')
    descr += 'mode: bynode' 
    descr += 'N: %i' %(run['nodes']) 
    descr += 'hostfile: auto' 
    descr += 'job_name: %s' %(run['jobname'])
    return descr


def Generate_Job(run,balrog,db,tiles,  where, setup, shifter):
    scmds = ''
    start = 0

    config = {}
    config['balrog'] = balrog
    config['db'] = db

    if where=='wq':
        descr = wqDirectives(run)
        space = "   "
        cmd, start = GetMainWork(setup, run, tiles, config, run['jobdir'], shifter, space=space, q=where, scmds=scmds, start=start)
        out = cmd + descr
        out = 'command: |\n' + out
        jobfile = os.path.join(run['jobdir'], '%s.wq' %(run['jobname']))
        WriteOut(jobfile, out)

    elif where=='slurm':
        allnodes = run['nodes']
        jobname = run['jobname']
        for k in range(run['ndependencies']):
            jobdir = GetDepJobDir(run, jobname, k=k)
            descr, scmds = SlurmDirectives(run, config, allnodes, jobdir, shifter)
            cmd, start = GetMainWork(setup, run, tiles, config, jobdir, shifter, q=where, scmds=scmds, start=start)
            out = descr + cmd
            jobfile = os.path.join(jobdir, '%s.sl' %(run['jobname']))
            WriteOut(jobfile, out)

    if run['ndependencies'] > 1:
        jobfile = WriteDepsJob(run, jobname)

    return jobfile


def GetWhere(argv):
    setup = None
    where = argv[1]
    config = argv[2]

    if len(argv) > 3:
        setup = argv[3]

    return where, setup, config


def GenJob(argv):
    where, setup, config = GetWhere(argv)
    run, balrog, db, tiles, shifter = GetConfig(where, config)
    job = Generate_Job(run,balrog,db,tiles, where, setup, shifter)
    return job, where


if __name__ == "__main__":

    job, where = GenJob(sys.argv)
    print 'Wrote job file to:', str(job)
