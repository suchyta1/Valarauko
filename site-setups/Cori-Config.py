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
    tiles = esutil.io.read(os.path.join(dir, 'spt-sva1+y1a1-overlap-grizY.fits'))['tilename']

    run['release'] = 'y1a1_coadd'
    #run['setup'] = os.path.join( os.path.dirname(os.path.realpath(__file__)), 'setups', 'y1-cori')

    run['outdir'] = os.path.join(os.environ['SCRATCH'],'BalrogScratch')
    run['db-columns'] = os.path.join(dir, 'y1a1_coadd_objects-columns.fits')
    run['balrog'] = os.path.join(os.environ['BALROG_DIR'], 'balrog.py')
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
    run, balrog, tiles = Y1A1Setup(run, balrog, tiles)

    tstart = 175
    tend = 250
    tiles = tiles[tstart:tend]

    run['command'] = 'popen'
    run['useshell'] = False

    run['nodes'] = len(tiles)
    run['ppn'] = 48
    run['walltime'] = '20:00:00'
    run['queue'] = 'regular'
    run['label'] = 'y1a1_spto'
    run['runnum'] = 0 

    tiletotal = 100000
    run['indexstart'] = tstart * tiletotal
    run['tiletotal'] = tiletotal
    balrog['ngal'] = 200

    run['DBoverwrite'] = False
    run['verifyindex'] = True
    run['joblabel'] = '%i:%i' %(tstart, tend)

    return run, balrog, db, tiles

