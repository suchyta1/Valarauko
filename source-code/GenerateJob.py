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
    run['queue'] = 'regular' # Which queue to use if running at NERSC. 
    run['email'] = None # Set this to your email if running at BNL to get an email at finish. At NERSC, I've set slurm to send start, 50%, and finish emails automatically
    run['asarray'] = False
    run['ppn'] = None
    run['downsample'] = None
    run['shifter'] = None
    run['slr'] = None
    run['cores'] = None

    run['DBoverwrite'] =  False  # Overwrite DB tables with same names (if they exist). False means append into existing tables. Regardless, the tables will be created if they don't exist.
    run['duplicate'] = None
    run['allfail'] = True
    #'verifyindex': True, # Check if you're trying to add balrog_index which already exists

    if where=='slurm':
        run['stripe'] = 2
        run['npersubjob'] = 1
        run['asdependency'] = None
        run['arraymax'] = None
    else:
        run['npersubjob'] = 1
        run['asdependency'] = False


    # will get passed as command line arguments to balrog
    balrog = RunConfigurations.BalrogConfigurations.default

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
    run['retry'] = True

    balrog['systemcmd'] = run['command']
    balrog['retrycmd'] = run['retry']
    balrog['useshell'] = run['useshell']
  
    if (run['shifter'] is not None):
        print "You're using shifter, super cool. I've detected shifter=%s. I'm configuring a bunch of stuff for you automatically that I don't let you overwrite."%(run['shifter'])
        shifter = shiftermodule.GetShifter(run,balrog)
        if run['slr'] is None:
            Exit( "You need to set the slr directory with shifter, because I can't make this public" )
    else:
        shifter = None

    mod = len(tiles) % (run['nodes'] * run['npersubjob'])
    if (mod != 0):
        Exit( "I only allow you to run a evenly divisible jobs (meaning len(tiles)/(nodes*npersubjob)=integer) in these wrappers because that's easier to understand, and doesn't really let you do any less." )
    run['ndependencies'] = len(tiles) / (run['nodes'] * run['npersubjob'])

    if where=='wq':
        if (run['ndependencies'] != 1):
            Exit( "With wq, I require len(tiles)/(nodes*npersubjob)=1. There's basically no reason not to run like this." )
        if run['asarray']:
            Exit( "You cannot do job arrays with wq. Must use asarray=False." )
        if run['asdependency']:
            Exit( "You cannot do job dependencies with wq. Must use asdependency=False." )

    if where=='slurm':
        if run['asarray']:
            if run['asdependency']:
                Exit( "Cannot use asarray and asdependency. These are mutually exclusive in my SLURM setup." )
            if (run['nodes'] != 1):
                Exit( "Job arrays must be run with nodes=1 in my scheme." )

        if not run['asarray']:
            if run['asdependency'] is None:
                run['asdependency'] = True

            if not run['asdependency']:
                Exit( "Have to choose either asdpendency or asarray. Both are False right now. If you don't say anything about either in your config file, you get asdependency." )

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


def EndBNL(s):
    return '%s\n   '%(s)

def EndNERSC(s):
    return '%s\n'%(s)

def GetEnd(s, end):
    if end=='wq':
        s = EndBNL(s)
    elif end=='slurm':
        s = EndNERSC(s)
    return s


def BalrogDir(run):
    d = ''
    if run['shifter'] is None:
        dir = os.path.dirname( os.path.realpath(run['balrog']) )
        d = "export PYTHONPATH=%s:${PYTHONPATH}"%(dir)
        d = GetEnd(d, 'slurm')
    return d

def Source(setup, end, run):
    s = ''
    if (setup is not None) and (run['shifter'] is None):
        s = 'source %s' %(setup)
        s = GetEnd(s, end)
    return s


def SLURMadd(str, val, start='#SBATCH'):
    str = str + '\n%s %s' %(start, val)
    return str


def WriteJson(config,dirname, tiles,start,end):
    jsonfile = os.path.join(dirname, '%s.json'%(runtile.Files.json))
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
    runcopy = copy.copy(run)

    if shifter is not None:
        runcopy['outdir'] = NewOutdir(runcopy, shifter)
        depdir = shifter.jobroot
        subdir = runtile.GetJsonDir(run, depdir, id)
        runcopy['pos'] = shifter.posroot
        runcopy['slr'] = shifter.slrroot
        runcopy['exitfile'] = os.path.join(subdir, runtile.Files.exit)
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


def CheckFails(exitfiles, space=''):
    files = ' '.join(exitfiles)
    check = """%sfails=0; files=""; for f in %s; do read -r result < $f; if [ "$result" = "1" ]; then let "fails+=1"; if [ $fails = "1" ]; then files="$f"; else files="${files},${f}"; fi; fi; done;\n"""%(space,files)
    exit = """%sif [ $fails = "0" ]; then echo "job succeeded"; code=0; else echo "job failed -- $fails failures -- bad exit files: $files"; code=1; fi\n"""%(space)
    return check, exit


def GetDepJobDir(run, k=0):
    jobdir = run['jobdir']
    if run['ndependencies'] > 1:
        run['jobname'] = '%s_%s_%i'%(run['jobname'],runtile.Files.depstr,k+1)
        jobdir = os.path.join(jobdir, '%s_%i'%(runtile.Files.depstr,k+1))
        TryToMake(jobdir)
    return jobdir


def Generate_Job(run,balrog,db,tiles,  where, setup, shifter):

    if run['shifter'] is None:
        thisdir = os.path.dirname(os.path.realpath(__file__))
    else:
        thisdir = shifter.thisdir
    allmpi = os.path.join(thisdir, 'RunTileJob.py')
    sendmail = os.path.join(thisdir, 'SendEmail.py')

    s = Source(setup, where, run)
    d = BalrogDir(run)
    start = 0

    config = {}
    config['balrog'] = balrog
    config['db'] = db
    deps = []
    exits = []


    if where=='wq':
        
        FindCreateFiles(run)
        space = "   "
        descr = 'mode: bynode\n' + 'N: %i\n' %(run['nodes']) + 'hostfile: auto\n' + 'job_name: %s' %(run['jobname'])
        cmd = space + """nodes=(); while read -r line; do found=false; host=$line; for h in "${nodes[@]}"; do if [ "$h" = "$host" ]; then found=true; fi; done; if [ "$found" = "false" ]; then nodes+=("$host"); fi; done < %hostfile%\n"""

        for i in range(run['nodes']):
            jsonfile, start = SubConfig(start,i, tiles, run,config, run['jobdir'], shifter=shifter)
            cmd = cmd + space + 'mpirun -np 1 -host ${nodes[%i]} %s %s &\n' %(i, allmpi, jsonfile)
            exits.append('"%s"'%(run['exitfile']))
        cmd = cmd + space + 'wait\n'

        check,exit = CheckFails(exits, space=space)
        cmd  = cmd + check
        cmd = cmd + exit
        if run['email'] is not None:
            cmd = cmd + space + '%s %s %s $code\n'%(sendmail, run['email'], run['jobname'])
        cmd = cmd + space + 'exit $code\n'

        out = 'command: |\n' + space + '%s%s%s%s' %(s, d, cmd, descr)
        jobfile = os.path.join(run['jobdir'], '%s.wq' %(run['jobname']))
        WriteOut(jobfile, out)

    elif where=='slurm':
        corestr = ''
        if run['cores'] is not None:
            corestr = ' -c %i'%(run['cores'])

        run['email'] = None
        allnodes = run['nodes']
        for k in range(run['ndependencies']):
            
            if k > 0:
                run['DBoverwrite'] = False
            jobdir = GetDepJobDir(run, k)

            descr = "#!/bin/bash -l \n"
            if run['shifter'] is not None:
                img = '--image=docker:%s'%(run['shifter'])
                descr = SLURMadd(descr, img, start='#SBATCH')
                netrc = os.path.join(os.environ['HOME'])
                #scmds = 'shifter %s --volume=%s:%s --volume=%s:%s --volume=%s:%s --volume=%s:%s --volume=%s:%s'%(img, jobdir,shifter.jobroot, run['outdir'],shifter.outroot, netrc,shifter.homeroot, run['slr'],shifter.slrroot, run['pos'],shifter.posroot)
                scmds = 'shifter %s --volume=%s:%s --volume=%s:%s --volume=%s:%s --volume=%s:%s'%(img, jobdir,shifter.jobroot, netrc,shifter.homeroot, run['slr'],shifter.slrroot, run['pos'],shifter.posroot)

            descr = SLURMadd(descr, '--job-name=%s'%(run['jobname']), start='#SBATCH')
            descr = SLURMadd(descr, '--mail-type=BEGIN,END,TIME_LIMIT_50', start='#SBATCH')
            descr = SLURMadd(descr, '--partition=%s'%(run['queue']), start='#SBATCH')
            descr = SLURMadd(descr, '--time=%s'%(run['walltime']), start='#SBATCH')

            if run['asarray']:
                ofile = os.path.join(jobdir, '%s_%%a'%(runtile.Files.substr), '%s-%%A_%%a.out'%(run['jobname']))
                arrmax = ''
                if run['arraymax'] is not None:
                    arrmax = '%%%i'%(run['arraymax'])
                descr = SLURMadd(descr, '--array=1-%i%s'%(len(tiles),arrmax), start='#SBATCH')
                descr = SLURMadd(descr, '--nodes=%i'%(allnodes), start='#SBATCH')
            else:
                ofile = os.path.join(jobdir, '%s-%%j.out'%(run['jobname']))
                descr = SLURMadd(descr, '--nodes=%i'%(allnodes), start='#SBATCH')

            descr = SLURMadd(descr, '--output=%s'%(ofile), start='#SBATCH')
            descr = descr + '\n\n'
            descr =  descr + s + d

            if run['shifter'] is not None:
                descr = descr + 'module load shifter\n'
            if run['stripe'] is not None:
                descr = descr + 'if ! [ -d %s ]; then mkdir %s; fi;\n' %(run['outdir'],run['outdir'])
                descr = descr + 'lfs setstripe %s --count %i\n' %(run['outdir'],run['stripe'])

            for i in range(run['nodes']):
                jsonfile, start = SubConfig(start,i, tiles, run,config, jobdir, shifter=shifter)
                jdir = os.path.dirname(jsonfile)
                exits.append('"%s"'%(run['exitfile']))

                if not run['asarray']:
                    if shifter is not None:
                        descr = descr + 'srun -N 1 -n 1%s %s /bin/bash -c "source /home/user/.bashrc; %s %s" &\n' %(corestr, scmds, allmpi, jsonfile)
                    else:
                        descr = descr + 'srun -N 1 -n 1%s %s %s &\n' %(corestr, allmpi, jsonfile)

                
            if run['asarray']:
                if run['shifter'] is None:
                    subdir = os.path.join(jobdir, '%s_${SLURM_ARRAY_TASK_ID}'%(runtile.Files.substr))
                else:
                    subdir = os.path.join(sjobdir, '%s_${SLURM_ARRAY_TASK_ID}'%(runtile.Files.substr))

                descr = descr + 'j=%s\n'%(os.path.join(subdir,'config.json'))
                #descr = descr + 'l=%s\n'%(os.path.join(subdir,'runlog'))
                #out = descr + 'srun -N 1 -n 1 %s ${j} ${l}' %(allmpi)
                out = descr + 'srun -N 1 -n 1 %s /bin/bash -c "source /home/user/.bashrc; %s ${j}"' %(scmds, allmpi)

            out = descr + 'wait\n'
            check,exit = CheckFails(exits, space='')
            out = out + check
            out = out + exit
            out = out + 'exit $code\n'

            jobfile = os.path.join(jobdir, '%s.sl' %(run['jobname']))
            WriteOut(jobfile, out)
            deps.append(jobfile)

    if run['ndependencies'] > 1:
        deps = ' '.join(deps)
        submit = os.path.join(run['jobdir'], 'submit.sh')
        t = '    '
        with open(submit, 'w') as out:
            out.write('deps="%s"\n'%(deps))
            out.write('arr=($deps)\n')
            out.write('for i in "${arr[@]}"; do\n')
            out.write('%sif [ "$i" = "${arr[0]}" ]; then\n'%(t))
            out.write('%s%scmd="sbatch $i"\n'%(t,t))
            out.write('%selse\n'%(t))
            out.write('%s%scmd="sbatch --dependency=afterok:$dep $i"\n'%(t,t))
            out.write('%sfi\n'%(t))
            out.write('%sout="$($cmd)"\n'%(t))
            out.write('%soutarr=($out)\n'%(t))
            out.write('%sdep=${outarr[${#outarr[@]}-1]}\n'%(t))
            out.write('done')
        os.chmod(submit, 0755)
        jobfile = submit

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
