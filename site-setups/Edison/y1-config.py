import os
import esutil
import imp 

dir = os.path.dirname(os.path.dirname(os.path.dirname( os.path.realpath(__file__) )))
BuildJob = imp.load_source('BuildJob', os.path.join(dir,'BuildJob.py'))


def Y1A1Setup(run, balrog, tiles):
    dir = os.environ['Y1A1_DIR']

    '''
    tiles = esutil.io.read(os.path.join(dir, 'spt-y1a1-only-g70-grizY.fits'))['tilename']
    run['pos'] = os.path.join(dir,'spt-y1a1-only-g70-grizY-pos-tile')
    '''
    tiles = esutil.io.read(os.path.join(dir,'y1a1-sptw1-grizY.fits'))['tilename']
    run['pos'] = os.path.join(dir,'y1a1-sptw1-grizY-tile-100000')


    run['release'] = 'y1a1_coadd'
    run['db-columns'] = os.path.join(dir, 'y1a1_coadd_objects-columns.fits')
    run['balrog'] = os.path.join(os.environ['LOCAL'], 'software', 'Balrog', 'balrog.py')

    #balrog['pyconfig'] = os.path.join(dir, 'Y1-only.py')
    balrog['pyconfig'] = os.path.join( os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))), 'pyconfig', 'y1a1.py')
    balrog['catalog'] = os.path.join(dir, 'CMC_originalR_v1.fits')
    balrog['slrdir'] = dir

    run['swarp-config'] = os.path.join(dir, '20150806_default.swarp')
    balrog['sexnnw'] = os.path.join(dir, '20150806_sex.nnw')
    balrog['sexconv'] = os.path.join(dir, '20150806_sex.conv')
    balrog['sexparam'] = os.path.join(dir, '20150806_sex.param_diskonly')
    balrog['nosimsexparam'] = os.path.join(dir, '20150806_sex.param_diskonly')
    balrog['sexconfig'] = os.path.join(dir, '20150806_sex.config')

    return run, balrog, tiles


def CustomConfig(run, balrog, db, tiles):
    run, balrog, tiles = Y1A1Setup(run, balrog, tiles)
    run = BuildJob.TrustEric(run, where='edison')
    run['duplicate'] = None
    run['ppn'] = 24

    tstart = 635
    tend = 785
    tiles = tiles[tstart:tend]

    run['nodes'] = 75
    run['walltime'] = '10:00:00'
    run['queue'] = 'regular'
    run['npersubjob'] = 2
    
    #baseout = os.environ['SCRATCH']
    baseout = '/scratch3/scratchdirs/esuchyta/'
    run['dbname'] = 'y1a1_sptw_01'
    run['joblabel'] = '%i-%i' %(tstart, tend)
    run['jobdir'] = os.path.join(baseout, 'BalrogJobs')
    run['outdir'] = os.path.join(baseout, 'BalrogScratch')

    balrog['ngal'] = 1000
    #run['downsample'] = balrog['ngal'] * run['ppn']
    run['runnum'] = 0 
    run['DBoverwrite'] = False

    return run, balrog, db, tiles
