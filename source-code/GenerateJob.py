#!/usr/bin/env python

import imp
import os
import sys
import esutil
import json
import datetime
import numpy as np
import copy

updir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
RunConfigurations = imp.load_source('RunConfigurations', os.path.join(updir,'RunConfigurations.py'))
#import RunConfigurations


def TryToMake(dir):
    if not os.path.exists(dir):
        try:
            os.makedirs(dir)
        except:
            print 'Could not make %s'%(dir)
            sys.exit(1)


# get a default config object
def GetConfig(where, config):

    # arguments for configuring the run
    run = RunConfigurations.RunConfigurations.default

    # hide these from user
    run['DBload'] = 'cx_Oracle'  # ['cx_Oracle', 'sqlldr'] How to write to DB. sqlldr is deprecated, and I don't guarantee it still works.
    run['bands'] = ['g','r','i','z','Y'] # Bands you'll get measurement catalogs for. I haven't tested changing this -- don't.
    run['dualdetection'] = [1,2,3]  # Use None not to use detection image. Otherwise the indices in the array of bands. I haven't tested changing this -- don't.
    run['intermediate-clean'] = True # Delete an iteration's output Balrog images
    run['tile-clean'] = True  # Delete the entire outdir/run's contents
    run['queue'] = 'regular' # Which queue to use if running at NERSC. 
    run['email'] = None # Set this to your email if running at BNL to get an email at finish. At NERSC, I've set slurm to send start, 50%, and finish emails automatically

    run['balrog_as_function'] = True
    run['command'] = 'popen' #['system', 'popen']
    run['useshell'] = False # Only relevant with popen
    run['retry'] = True


    # These are only relevant at NERSC. Ask Eric Suchyta what in the world these ones do. They're a little less trivial, and change the workflow.
    run['stripe'] = None 
    run['sequential'] = False
    run['asarray'] = False
    run['arraymax'] = None
    run['npersubjob'] = 0


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

    if run['sequential'] and run['asarray']:
        print 'Cannot set both sequential and asarray to be True'
        sys.exit(1)

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


def BalrogDir(run):
    dir = os.path.dirname( os.path.realpath(run['balrog']) )
    d = "export PYTHONPATH=%s:${PYTHONPATH}"%(dir)
    d = GetEnd(d, 'EDISON')
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


def WriteJson(config,dirname, tiles,start,end):
    jsonfile = os.path.join(dirname, 'config.json')
    config['tiles'] = list(tiles[start:end])
    with open(jsonfile, 'w') as outfile:
        json.dump(config, outfile)
    return jsonfile


def GetJdir(usesub, dirname, id, substr):
    if usesub:
        jdir = os.path.join(dirname, '%s_%i'%(substr,id))
    else:
        jdir = dirname

    if not os.path.exists(jdir):
        os.makedirs(jdir)
    return jdir


def SubConfig(start,i,indexstart, tiles,subtiles,subnodes,run,config, usesub,substr):
    end = start + subtiles[i]
    id = i + 1
    jdir = GetJdir(usesub, run['jobdir'], id, substr)
    logdir = os.path.join(jdir, 'runlog')
    
    run['indexstart'] = indexstart + start*run['tiletotal']
    run['nodes'] = subnodes[i]
    config['run'] = run
    jsonfile = WriteJson(config,jdir, tiles,start,end)

    num = run['nodes'] * run['ppn']
    return jsonfile, logdir, num, end


def Generate_Job(run,balrog,db,tiles,  where, setup, subtiles, subnodes, usesub):

    thisdir = os.path.dirname(os.path.realpath(__file__))
    allmpi = os.path.join(thisdir, 'AllMpi.py')
    jobfile = os.path.join(run['jobdir'], run['jobname'])
    s = Source(setup, where)
    d = BalrogDir(run)
    seq = " &"
    if run['sequential']:
        seq = ''

    substr = 'subjob'
    config = {}
    config['balrog'] = balrog
    config['db'] = db

    if where.upper()=='BNL':

        descr = 'mode: bynode\n' + 'N: %i\n' %(run['nodes']) + 'hostfile: auto\n' + 'job_name: %s' %(run['jobname'])
        indexstart = copy.copy(run['indexstart'])
        start = 0
        #ss = 1

        st = 0 
        cmd = """   nodes=(); while read -r line; do found=false; host=$line; for h in "${nodes[@]}"; do if [ "$h" = "$host" ]; then found=true; fi; done; if [ "$found" = "false" ]; then nodes+=("$host"); fi; done < %hostfile%\n"""

        for i in range(len(subtiles)):
            jsonfile, logdir, num, start = SubConfig(start,i,indexstart, tiles,subtiles,subnodes,run,config, usesub,substr)

            et = st + subnodes[i] - 1
            cmd = cmd + """   nstart=%i; nend=%i; for i in `seq $nstart $nend`; do for j in `seq 0 %i`; do  if [ $i = $nstart ] && [ $j = 0 ]; then host=${nodes[$i]}; else host="${host},${nodes[$i]}"; fi; done; done;\n"""%(st, et, run['ppn']-1)
            cmd = cmd + '   mpirun -np %i -host $host %s %s %s%s\n' %(num, allmpi, jsonfile, logdir, seq)

            if not run['sequential']:
                st = et + 1

            #ee = ss + subnodes[i]*run['ppn']
            #cmd = cmd + """   start=%i; end=%i; for i in `seq $start $end`; do h=`sed -n "${i}{p;q;}" %%hostfile%%`; if [ $i = $start ]; then hosts=$h; else hosts="${hosts},${h}"; fi; done;\n"""%(ss,ee)
            #cmd = cmd + '   mpirun -npernode %i -np %i -host $hosts %s %s %s &\n' %(run['ppn'], num, allmpi, jsonfile, logdir)
            #ss = ee

            #cmd = cmd + '   mpirun -npernode %i -np %i -hostfile %%hostfile%% %s %s %s\n' %(run['ppn'], num, allmpi, jsonfile, logdir)

        cmd = cmd + '   wait\n'
        out = 'command: |\n   %s%s%s%s' %(s, d, cmd, descr)
        jobfile = '%s.wq' %(jobfile)
    

    elif where.upper() in ['CORI', 'EDISON']:
        run['email'] = None
        descr = "#!/bin/bash -l \n"

        descr = SLURMadd(descr, '--job-name=%s'%(run['jobname']), start='#SBATCH')
        descr = SLURMadd(descr, '--mail-type=BEGIN,END,TIME_LIMIT_50', start='#SBATCH')
        descr = SLURMadd(descr, '--partition=%s'%(run['queue']), start='#SBATCH')
        descr = SLURMadd(descr, '--time=%s'%(run['walltime']), start='#SBATCH')

        if run['asarray']:
            ofile = os.path.join(run['jobdir'], '%s_%%a'%(substr), '%s-%%A_%%a.out'%(run['jobname']))
            arrmax = ''
            if run['arraymax'] is not None:
                arrmax = '%%%i'%(run['arraymax'])
            descr = SLURMadd(descr, '--array=1-%i%s'%(len(subtiles),arrmax), start='#SBATCH')
            maxnodes = np.amax(subnodes)
            descr = SLURMadd(descr, '--nodes=%i'%(maxnodes), start='#SBATCH')
            if not (np.all(subnodes==maxnodes)):
                print 'In job arrays, each subjob must use the same number of nodes. You gave a "non-equally divisible" job, chunked into subjobs of node sizes: %s. Setting job array to use nodes=%i'%(str(subnodes),maxnodes)
        else:
            ofile = os.path.join(run['jobdir'], '%s-%%j.out'%(run['jobname']))
            descr = SLURMadd(descr, '--nodes=%i'%(run['nodes']), start='#SBATCH')

        descr = SLURMadd(descr, '--output=%s'%(ofile), start='#SBATCH')
        descr = descr + '\n\n'
        descr =  descr + s + d

        if run['stripe'] is not None:
            descr = descr + 'if ! [ -d %s ]; then mkdir %s; fi;\n' %(run['outdir'],run['outdir'])
            descr = descr + 'lfs setstripe %s --count %i\n' %(run['outdir'],run['stripe'])

        indexstart = copy.copy(run['indexstart'])
        start = 0
        for i in range(len(subtiles)):
            jsonfile, logdir, num, start = SubConfig(start,i,indexstart, tiles,subtiles,subnodes,run,config, usesub,substr)
            jdir = os.path.dirname(jsonfile)

            if not run['asarray']:
                descr = descr + 'srun -N %i -n %i %s %s %s%s\n' %(subnodes[i], num, allmpi, jsonfile, logdir,seq)
            else:
                nodefile = os.path.join(jdir, 'N')
                npfile = os.path.join(jdir, 'n')
                with open(nodefile, 'w') as f:
                    f.write('%i'%(subnodes[i]))
                with open(npfile, 'w') as f:
                    f.write('%i'%(num))
            
        if run['asarray']:
            subdir = os.path.join(run['jobdir'], '%s_${SLURM_ARRAY_TASK_ID}'%(substr))
            descr = descr + 'N=$(head -n 1 %s)\n'%(os.path.join(subdir,'N'))
            descr = descr + 'n=$(head -n 1 %s)\n'%(os.path.join(subdir,'n'))
            descr = descr + 'j=%s\n'%(os.path.join(subdir,'config.json'))
            descr = descr + 'l=%s\n'%(os.path.join(subdir,'runlog'))
            out = descr + 'srun -N ${N} -n ${n} %s ${j} ${l}' %(allmpi)
        else:
            out = descr + 'wait'
        jobfile = '%s.sl' %(jobfile)


    with open(jobfile, 'w') as job:
        job.write(out)

    return jobfile


def GetWhere(argv):
    setup = None
    where = argv[1]
    config = argv[2]

    if len(argv) > 3:
        setup = argv[3]

    return where, setup, config


def ConservativeDivide(num, den):
    inc = 0
    if (num % den > 0):
        inc = 1
    return num/den + inc
  


def Reallocate(subnodes, subtiles, nodes):
    if nodes < len(subnodes):
        print 'Cannot divide %i node(s) into %i simultaneous subjobs'%(nodes,len(subnodes))
        sys.exit(1)

    effective = (subtiles/subnodes) + np.int32(np.mod(subtiles, subnodes) > 0)

    if (np.sum(subnodes) > nodes):
        changeby = -1
    else:
        changeby = 1

    change = 0
    while np.sum(subnodes) != nodes:
        new_effective = ConservativeDivide(subtiles[change],subnodes[change]+changeby)
        if not np.any( np.fabs(effective - new_effective) > 1):
            subnodes[change] = subnodes[change] + changeby

        change += 1
        if change==len(subnodes):
            change = 0

    return subnodes



def BySub(tiles, run, where):
    usesub = False
    if run['npersubjob'] <= 0:
        nsub = 1
    elif run['npersubjob'] >= len(tiles):
        nsub = 1
    else:
        nsub = ConservativeDivide(len(tiles),run['npersubjob'])
        usesub = True

    subtiles = np.append(np.array( [run['npersubjob']]*(nsub-1), dtype=np.int32 ), len(tiles)-(nsub-1)*run['npersubjob'])
    if run['sequential']:
        subnodes = np.array( [run['nodes']]*len(subtiles) )
    else:
        target = ConservativeDivide(len(tiles), run['nodes'])
        subnodes = (subtiles/target) + np.int32(np.mod(subtiles,target) > 0)
        subnodes = Reallocate(subnodes, subtiles, run['nodes'])

    return subtiles, subnodes, usesub


def GenJob(argv):
    where, setup, config = GetWhere(argv)
    run, balrog, db, tiles = GetConfig(where, config)
    subtiles, subnodes, usesub = BySub(tiles, run, where)



    job = Generate_Job(run,balrog,db,tiles, where, setup, subtiles, subnodes, usesub)
    return job, where


if __name__ == "__main__":

    job, where = GenJob(sys.argv)
    print 'Wrote job file to:', job
