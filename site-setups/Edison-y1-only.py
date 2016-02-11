import os
import esutil


def SVA1Setup(run, balrog):
    run['release'] = 'sva1_coadd'
    run['module_setup'] = 'balrog_sva1_setup'
    run['outdir'] = os.path.join(os.environ['SCRATCH'],'BalrogScratch')

    balrog['pyconfig'] = os.path.join(os.environ['BALROG_MPI'], 'pyconfig', 'slr2.py')
    run['swarp-config'] = os.path.join(os.environ['ASTRO_CONFIG'], 'default.swarp') 
    balrog['sexnnw'] = os.path.join(os.environ['ASTRO_CONFIG'],'sex.nnw')
    balrog['sexconv'] = os.path.join(os.environ['ASTRO_CONFIG'], 'sex.conv')
    balrog['sexparam'] = os.path.join(os.environ['ASTRO_CONFIG'], 'sex.param_diskonly')
    balrog['nosimsexparam'] = os.path.join(os.environ['ASTRO_CONFIG'], 'sex.param_diskonly')
    balrog['sexconfig'] = os.path.join(os.environ['ASTRO_CONFIG'], 'sex.config')

    return run, balrog


def Y1A1Setup(run, balrog, tiles):
    dir = os.environ['BALROG_CONFIG']
    tiles = esutil.io.read(os.path.join(dir, 'spt-y1a1-only-g70-grizY.fits'))['tilename']

    run['release'] = 'y1a1_coadd'
    run['db-columns'] = os.path.join(dir, 'y1a1_coadd_objects-columns.fits')
    run['balrog'] = os.path.join(os.environ['BALROG_DIR'], 'balrog.py')
    balrog['pyconfig'] = os.path.join(dir, 'Y1-only.py')

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

    run['nodes'] = len(tiles)
    run['ppn'] = 24
    run['walltime'] = '20:00:00'
    run['queue'] = 'regular'
    run['runnum'] = 0 

    run['stripe'] = 2
    run['sequential'] = False
    run['asarray'] = False
    run['arraymax'] = None
    run['npersubjob'] = 0

    baseout = '/scratch3/scratchdirs/esuchyta/'
    run['dbname'] = 'y1a1_sptn_01'
    run['joblabel'] = '%i:%i' %(tstart, tend)
    run['jobdir'] = os.path.join(baseout, 'BalrogJobs')
    run['outdir'] = os.path.join(baseout, 'BalrogScratch')

    balrog['ngal'] = 1000
    run['tiletotal'] = 4 * balrog['ngal'] * (run['ppn']-1) 
    run['indexstart'] = tstart * run['tiletotal']

    run['DBoverwrite'] = False
    run['verifyindex'] = True

    return run, balrog, db, tiles
