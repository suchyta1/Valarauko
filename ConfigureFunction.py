#!/usr/bin/env python

import RunConfigurations
import os
import esutil
import CustomConfig


# change the defaults if you want
def NerscConfig(run, balrog, DESdb, db, tiles):
    run['label'] = 'debug_nersc'
    run['tiletotal'] = 2000
    run['DBoverwrite'] = True
    run['DBload'] = 'cx_Oracle'
    run['bands'] = ['i']
    run['dualdetection'] = None

    balrog = pyconfig(balrog)
    tiles = tiles[0:2]
    return run, balrog, DESdb, db, tiles


# change the defaults if you want
def BNLConfig(run, balrog, DESdb, db, tiles):
    #run['label'] = 'debug_bnl'
    #run['label'] = 'des_sva1'
    run['label'] = 'sva1_des'
    run['tiletotal'] = 100000
    run['DBoverwrite'] = True
    run['DBload'] = 'cx_Oracle'
    #run['DBload'] = 'sqlldr'
    run['nomulti'] = False

    balrog = pyconfig(balrog)
    tiles = tiles[0:30]

    return run, balrog, DESdb, db, tiles


def pyconfig(balrog):
    #balrog['sexparam'] = os.path.join(os.environ['BALROG_MPI_ASTRO_CONFIG'], 'sva1', 'sex.param')
    balrog['oldmorph'] = False
    if balrog['oldmorph']:
        balrog["reff"] = "HALF_LIGHT_RADIUS"
        balrog["sersicindex"] = "SERSIC_INDEX"
    return balrog


# get a default config object
def GetConfig(where):

    # arguments for configuring the run
    run = RunConfigurations.RunConfigurations.default

    # will get passed as command line arguments to balrog
    balrog = RunConfigurations.BalrogConfigurations.default

    # stuff for talking to the DESdb module for finding file
    DESdb = RunConfigurations.desdbInfo.sva1_coadd

    # DB connection info
    db = RunConfigurations.DBInfo.default

    # what files to run balrog over
    tileinfo = esutil.io.read('spte-tiles.fits')
    tiles = tileinfo['tilename']

    '''
    run['where'] = where
    if run['where']=='NERSC':
        run, balrog, DESdb, db, tiles = NerscConfig(run, balrog, DESdb, db, tiles)
    elif run['where']=='BNL':
        run, balrog, DESdb, db, tiles = BNLConfig(run, balrog, DESdb, db, tiles)
    '''
    run, balrog, DESdb, db, tiles = CustomConfig.CustomConfig(run, balrog, DESdb, db, tiles, where)

    #q = SubmitQueue(run)
    return run, balrog, DESdb, db, tiles


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
        cmd = 'cd $PBS_O_WORKDIR\nmpirun -np %i ./AllMpi.py %s' %(num, where)
        out = '%s%s' %(descr, cmd)

    job = open(filename, 'w')
    job.write(out)
    return filename

    #tiles = cur.quick("SELECT tile.tilename, tile.urall, tile.uraur, tile.udecll, tile.udecur from coaddtile tile   JOIN (select distinct(tilename) as tilename from sva1_coadd_spte) sva1 ON sva1.tilename=tile.tilename  ORDER BY tile.udecll DESC, tile.urall ASC", array=True)


# Runs
# 0-30

