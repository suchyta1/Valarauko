import os


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
    tiles = esutil.io.read(os.path.join(dir, 'spt-sva1+y1a1-overlap-grizY.fits'))['tilename']

    run['release'] = 'y1a1_coadd'
    run['module_setup'] = 'balrog_y1a1_setup'
    run['outdir'] = os.path.join(os.environ['SCRATCH'],'BalrogScratch')
    run['db-columns'] = os.path.join(dir, 'y1a1_coadd_objects-columns.fits')
    balrog['pyconfig'] = os.path.join(dir, 'BalrogConfig-OrigSGQ.py')

    run['swarp-config'] = os.path.join(dir, '20150806_default.swarp')
    balrog['sexnnw'] = os.path.join(dir, '20150806_sex.nnw')
    balrog['sexconv'] = os.path.join(dir, '20150806_sex.conv')
    balrog['sexparam'] = os.path.join(dir, '20150806_sex.param_diskonly')
    balrog['nosimsexparam'] = os.path.join(dir, '20150806_sex.param_diskonly')
    balrog['sexconfig'] = os.path.join(dir, '20150806_sex.config')

    return run, balrog, tiles



# change the defaults if you want
def CustomConfig(run, balrog, db, tiles):

    # Always check these
    run, balrog, tiles = Y1A1Setup(run, balrog, tiles)

    tiles = tiles[100:102]
    run['ppn'] = 24
    run['nodes'] = 2
    run['walltime'] = '00:30:00'
    run['queue'] = 'debug'

    run['label'] = 'y1test'
    run['joblabel'] = 'nersc'
    run['DBoverwrite'] = True

    run['tiletotal'] = 100
    balrog['ngal'] = 10

    return run, balrog, db, tiles
