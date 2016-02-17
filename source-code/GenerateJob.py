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


def Exit(msg):
    print msg
    sys.exit(1)

class constants:
    nersc = ['EDISON','CORI']

def TryToMake(dir):
    if not os.path.exists(dir):
        try:
            os.makedirs(dir)
        except:
            Exit( 'Could not make %s'%(dir) )


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
    run['sequential'] = False
    run['asarray'] = False

    if where.upper() in constants.nersc:
        run['stripe'] = 2
        run['npersubjob'] = 1
        run['asdependency'] = None
        run['arraymax'] = None
        if where.upper()=='EDISON':
            run['ppn'] = 24
        elif where.upper()=='CORI':
            run['ppn'] = 32
    else:
        stripe = None
        run['npersubjob'] = 0
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
    balrog['systemcmd'] = run['command']
    balrog['retrycmd'] = run['retry']
    balrog['useshell'] = run['useshell']
    
    if (where.upper()=='BNL'):
        if run['asarray']:
            Exit( 'You cannot do job arrays at BNL. Must use asarray=False.' )
        if run['asdependency']:
            Exit( 'You cannot do job dependencies with wq. Must use asdependency=False.' )

    if (where.upper() in constants.nersc):
        if run['asarray'] and run['asdependency']:
            Exit( "Cannot run asarray and asdependency" )

        if run['sequential']:
            Exit( "I don't allow you to use sequential at NERSC. This isn't technically impossible, but doesn't really make sense there." )

        mod = len(tiles) % (run['nodes'] * run['npersubjob'])
        div = len(tiles) / (run['nodes'] * run['npersubjob'])

        if (mod != 0):
            Exit( "I only allow you to run a evenly divisible jobs at NERSC." )
        
        '''
        if run['npersubjob'] > 1:
            print "You are using npersubjob > 1. Are you sure you want to do this? You'll need ngal <= 200, and your jobs are going to take A LOT longer."
        '''

        if not run['asarray']:
            if run['asdependency'] is None:
                run['asdependency'] = True

            if run['asdependency']:
                run['ndependencies'] = div 
            else:
                run['ndependencies'] = 1
                '''
                if (use != 1):
                    print "Your job is going to require nodes running more than one tile. Are you sure you want to do this?"
                '''

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
    elif end.upper() in constants.nersc:
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


def SubConfig(start,i,indexstart, tiles,subtiles,subnodes,run,config, usesub,substr, jobdir):
    end = start + subtiles[i]
    id = i + 1
    jdir = GetJdir(usesub, jobdir, id, substr)
    logdir = os.path.join(jdir, 'runlog')
    
    run['indexstart'] = indexstart + start*run['tiletotal']
    #run['nodes'] = subnodes[i]
    config['run'] = run
    jsonfile = WriteJson(config,jdir, tiles,start,end)

    #num = run['nodes'] * run['ppn']
    num = subnodes[i] * run['ppn']
    return jsonfile, logdir, num, end


def WriteOut(jobfile, out):
    with open(jobfile, 'w') as job:
        job.write(out)


def Generate_Job(run,balrog,db,tiles,  where, setup, subtiles, subnodes, usesub):

    thisdir = os.path.dirname(os.path.realpath(__file__))
    allmpi = os.path.join(thisdir, 'AllMpi.py')
    s = Source(setup, where)
    d = BalrogDir(run)
    seq = " &"
    if run['sequential']:
        seq = ''

    substr = 'subjob'
    config = {}
    config['balrog'] = balrog
    config['db'] = db
    deps = []

    if where.upper()=='BNL':

        descr = 'mode: bynode\n' + 'N: %i\n' %(run['nodes']) + 'hostfile: auto\n' + 'job_name: %s' %(run['jobname'])
        indexstart = copy.copy(run['indexstart'])
        start = 0
        #ss = 1

        st = 0 
        cmd = """   nodes=(); while read -r line; do found=false; host=$line; for h in "${nodes[@]}"; do if [ "$h" = "$host" ]; then found=true; fi; done; if [ "$found" = "false" ]; then nodes+=("$host"); fi; done < %hostfile%\n"""

        for i in range(len(subtiles[0])):
            jsonfile, logdir, num, start = SubConfig(start,i,indexstart, tiles,subtiles[0],subnodes[0],run,config, usesub,substr, run['jobdir'])

            et = st + subnodes[0][i] - 1
            cmd = cmd + """   nstart=%i; nend=%i; for i in `seq $nstart $nend`; do for j in `seq 0 %i`; do  if [ $i = $nstart ] && [ $j = 0 ]; then host=${nodes[$i]}; else host="${host},${nodes[$i]}"; fi; done; done;\n"""%(st, et, run['ppn']-1)
            cmd = cmd + '   mpirun -np %i -host $host %s %s %s%s\n' %(num, allmpi, jsonfile, logdir, seq)

            if not run['sequential']:
                st = et + 1

        cmd = cmd + '   wait\n'
        out = 'command: |\n   %s%s%s%s' %(s, d, cmd, descr)
        jobfile = os.path.join(run['jobdir'], '%s.wq' %(run['jobname']))
        WriteOut(jobfile, out)

    

    elif where.upper() in constants.nersc:
        run['email'] = None

        start = 0
        for k in range(run['ndependencies']):

            if run['ndependencies'] > 1:
                jobdir = '%s_%i'%(run['jobdir'],k+1)
                TryToMake(jobdir)
            else:
                jobdir = run['jobdir']

            descr = "#!/bin/bash -l \n"
            descr = SLURMadd(descr, '--job-name=%s'%(run['jobname']), start='#SBATCH')
            descr = SLURMadd(descr, '--mail-type=BEGIN,END,TIME_LIMIT_50', start='#SBATCH')
            descr = SLURMadd(descr, '--partition=%s'%(run['queue']), start='#SBATCH')
            descr = SLURMadd(descr, '--time=%s'%(run['walltime']), start='#SBATCH')

            if run['asarray']:
                ofile = os.path.join(jobdir, '%s_%%a'%(substr), '%s-%%A_%%a.out'%(run['jobname']))
                arrmax = ''
                if run['arraymax'] is not None:
                    arrmax = '%%%i'%(run['arraymax'])
                descr = SLURMadd(descr, '--array=1-%i%s'%(len(subtiles[k]),arrmax), start='#SBATCH')
                maxnodes = np.amax(subnodes[k])
                descr = SLURMadd(descr, '--nodes=%i'%(maxnodes), start='#SBATCH')
                if not (np.all(subnodes[k]==maxnodes)):
                    print 'In job arrays, each subjob must use the same number of nodes. You gave a "non-equally divisible" job, chunked into subjobs of node sizes: %s. Setting job array to use nodes=%i'%(str(subnodes[k]),maxnodes)
            else:
                ofile = os.path.join(jobdir, '%s-%%j.out'%(run['jobname']))
                descr = SLURMadd(descr, '--nodes=%i'%(run['nodes']), start='#SBATCH')

            descr = SLURMadd(descr, '--output=%s'%(ofile), start='#SBATCH')
            descr = descr + '\n\n'
            descr =  descr + s + d

            if run['stripe'] is not None:
                descr = descr + 'if ! [ -d %s ]; then mkdir %s; fi;\n' %(run['outdir'],run['outdir'])
                descr = descr + 'lfs setstripe %s --count %i\n' %(run['outdir'],run['stripe'])

            indexstart = copy.copy(run['indexstart'])
            for i in range(len(subtiles[k])):
                jsonfile, logdir, num, start = SubConfig(start,i,indexstart, tiles,subtiles[k],subnodes[k],run,config, usesub,substr, jobdir)
                jdir = os.path.dirname(jsonfile)

                if not run['asarray']:
                    descr = descr + 'srun -N %i -n %i %s %s %s%s\n' %(subnodes[k][i], num, allmpi, jsonfile, logdir,seq)
                else:
                    nodefile = os.path.join(jdir, 'N')
                    npfile = os.path.join(jdir, 'n')
                    with open(nodefile, 'w') as f:
                        f.write('%i'%(subnodes[k][i]))
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


def ConservativeDivide(num, den):
    inc = 0
    if (num % den > 0):
        inc = 1
    return num/den + inc
  


def Reallocate(subnodes, subtiles, nodes):
    if nodes < len(subnodes):
        Exit( 'Cannot divide %i node(s) into %i simultaneous subjobs'%(nodes,len(subnodes)) )

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

    ts = np.append(np.array( [run['npersubjob']]*(nsub-1), dtype=np.int32 ), len(tiles)-(nsub-1)*run['npersubjob'])
    subtiles = np.array( np.array_split(ts, run['ndependencies']) )
    subnodes = []

    if run['asarray']:
        run['nodes'] = run['nodes'] * len(ts)
    
    for i in range(run['ndependencies']):

        if run['sequential']:
            subnodes[i] = np.array( [run['nodes']]*len(subtiles[i]) )
        else:
            target = ConservativeDivide(np.sum(subtiles[i]), run['nodes'])
            sn = (subtiles[i]/target) + np.int32(np.mod(subtiles[i],target) > 0)
            r = Reallocate(sn, subtiles[i], run['nodes'])
            subnodes.append(r)

            if (where in constants.nersc) and  (np.sum(subnodes[i] > 1) > 0):
                print "You're job is going to require more than 1 node for a subjob. Are you sure you want to do this?"

    subnodes = np.array(subnodes)
    return subtiles, subnodes, usesub


def GenJob(argv):
    where, setup, config = GetWhere(argv)
    run, balrog, db, tiles = GetConfig(where, config)
    subtiles, subnodes, usesub = BySub(tiles, run, where)



    job = Generate_Job(run,balrog,db,tiles, where, setup, subtiles, subnodes, usesub)
    return job, where


if __name__ == "__main__":

    job, where = GenJob(sys.argv)
    print 'Wrote job file to:', str(job)
