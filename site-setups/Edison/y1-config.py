import os
import esutil


def Y1A1Setup(run, balrog, tiles):
    dir = os.environ['Y1A1_DIR']
    tiles = esutil.io.read(os.path.join(dir, 'spt-y1a1-only-g70-grizY.fits'))['tilename']
    run['pos'] = os.path.join(dir,'spt-y1a1-only-g70-grizY-pos')

    run['release'] = 'y1a1_coadd'
    run['db-columns'] = os.path.join(dir, 'y1a1_coadd_objects-columns.fits')
    balrog['pyconfig'] = os.path.join(dir, 'Y1-only.py')
    run['balrog'] = os.path.join(os.environ['LOCAL'], 'software', 'balrog.py')

    run['swarp-config'] = os.path.join(dir, '20150806_default.swarp')
    balrog['sexnnw'] = os.path.join(dir, '20150806_sex.nnw')
    balrog['sexconv'] = os.path.join(dir, '20150806_sex.conv')
    balrog['sexparam'] = os.path.join(dir, '20150806_sex.param_diskonly')
    balrog['nosimsexparam'] = os.path.join(dir, '20150806_sex.param_diskonly')
    balrog['sexconfig'] = os.path.join(dir, '20150806_sex.config')

    return run, balrog, tiles


# change the defaults if you want
def CustomConfig(run, balrog, db, tiles):
    run, balrog, tiles = Y1A1Setup(run, balrog, tiles)

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

    run['downsample'] = 50
    balrog['ngal'] = 10
    run['runnum'] = 0 

    run['DBoverwrite'] = True
    run['verifyindex'] = True

    return run, balrog, db, tiles
