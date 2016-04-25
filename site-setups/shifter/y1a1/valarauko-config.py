import os
import esutil
import imp

dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname( os.path.realpath(__file__) ))))
BuildJob = imp.load_source('BuildJob', os.path.join(dir,'BuildJob.py'))


def CustomConfig(run, balrog, db, tiles):
    run['shifter'] = 'esuchyta/valarauko:y1a1'

    run = BuildJob.TrustEric(run, where='edison')
    dir = '/scratch1/scratchdirs/esuchyta/software/balrog_config/y1a1/'
    run['ppn'] = 24
    #baseout = os.environ['SCRATCH']
    baseout = '/scratch3/scratchdirs/esuchyta/'

    '''
    run = BuildJob.TrustEric(run, where='cori')
    dir = '/global/cscratch1/sd/esuchyta/cori-software/balrog_config/y1a1/'
    run['ppn'] = 32
    baseout = os.environ['SCRATCH']
    '''

    tiles = esutil.io.read(os.path.join(dir, 'spt-y1a1-only-g70-grizY.fits'))['tilename']
    run['pos'] = os.path.join(dir,'spt-y1a1-only-g70-grizY-pos-tile')
    balrog['catalog'] = os.path.join(dir, 'CMC_originalR_v1.fits')
    balrog['slrdir'] = dir

    tstart = 0
    tend = 2
    tiles = tiles[tstart:tend]
    run['nodes'] = 2

    run['walltime'] = '00:30:00'
    run['queue'] = 'debug'
    run['runnum'] = 0     
    run['npersubjob'] = 1
    
    run['dbname'] = 'y1a1_stest'
    run['joblabel'] = '%i-%i' %(tstart, tend)
    run['jobdir'] = os.path.join(baseout, 'BalrogJobs')
    run['outdir'] = os.path.join(baseout, 'BalrogScratch')

    balrog['ngal'] = 10
    run['downsample'] = balrog['ngal']*run['ppn']
    run['DBoverwrite'] = True

    return run, balrog, db, tiles
