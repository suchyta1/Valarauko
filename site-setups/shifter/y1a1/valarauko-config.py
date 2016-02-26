import os
import esutil


# change the defaults if you want
def CustomConfig(run, balrog, db, tiles):
    run['shifter'] = 'esuchyta/balrog-docker:v1'

    dir = '/scratch1/scratchdirs/esuchyta/software/balrog_config/y1a1/'
    run['pos'] = os.path.join(dir,'spt-y1a1-only-g70-grizY-pos')
    run['slr'] = '/scratch1/scratchdirs/esuchyta/software/balrog_config/y1a1/'
    tiles = esutil.io.read(os.path.join(dir, 'spt-y1a1-only-g70-grizY.fits'))['tilename']

    tstart = 0
    tend = 4
    tiles = tiles[tstart:tend]

    run['nodes'] = 2
    run['ppn'] = 24
    run['walltime'] = '00:30:00'
    run['queue'] = 'debug'
    run['runnum'] = 0     
    
    run['npersubjob'] = 1
    run['asdependency'] = True
    
    baseout = '/scratch3/scratchdirs/esuchyta/'
    #baseout = os.environ['SCRATCH']
    run['dbname'] = 'y1a1_etest'
    run['joblabel'] = '%i-%i' %(tstart, tend)
    run['jobdir'] = os.path.join(baseout, 'BalrogJobs')
    run['outdir'] = os.path.join(baseout, 'BalrogScratch')


    run['downsample'] = 100
    balrog['ngal'] = 10
    run['runnum'] = 0 

    run['DBoverwrite'] = True
    run['duplicate'] = 'replace'
    run['allfail'] = True

    return run, balrog, db, tiles
