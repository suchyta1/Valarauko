import os
import esutil


# change the defaults if you want
def CustomConfig(run, balrog, db, tiles):

    dir = '/scratch1/scratchdirs/esuchyta/software/balrog_config/y1a1/'
    tiles = esutil.io.read(os.path.join(dir, 'spt-y1a1-only-g70-grizY.fits'))['tilename']
    tstart = 0
    tend = 1
    tiles = tiles[tstart:tend]

    run['nodes'] = 1
    run['ppn'] = 24
    run['walltime'] = '00:30:00'
    run['queue'] = 'debug'
    run['runnum'] = 0     
    
    run['npersubjob'] = 1
    run['asdependency'] = True
    
    #baseout = '/scratch3/scratchdirs/esuchyta/'
    baseout = os.environ['SCRATCH']
    run['dbname'] = 'y1a1_etest2'
    run['joblabel'] = '%i:%i' %(tstart, tend)
    run['jobdir'] = os.path.join(baseout, 'BalrogJobs')
    run['outdir'] = os.path.join(baseout, 'BalrogScratch')

    run['shifter'] = 'esuchyta/balrog-docker:v1'
    run['slr'] = '/scratch1/scratchdirs/esuchyta/software/balrog_config/y1a1/'
    run['pos'] = os.path.join(dir,'spt-y1a1-only-g70-grizY-pos')

    run['downsample'] = 50
    balrog['ngal'] = 10
    run['runnum'] = 0 

    run['DBoverwrite'] = True
    run['verifyindex'] = True

    return run, balrog, db, tiles
