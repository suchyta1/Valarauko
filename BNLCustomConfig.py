import os
import numpy as np
import sys
import esutil


def SVA1Setup(run, balrog):
    run['release'] = 'sva1_coadd'
    run['funpack'] = os.path.join(os.environ['BALROG_MPI'], 'software','cfitsio-3.300','funpack')
    run['swarp'] = os.path.join(os.environ['BALROG_MPI'], 'software','swarp-2.36.1','install-dir','bin','swarp')
    run['swarp-config'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'default.swarp')
    run['outdir'] = os.path.join(os.environ['SCRATCH'],'BalrogScratch')

    run['balrog'] = os.path.join(os.environ['BALROG_MPI'], 'software','Balrog','balrog.py')
    balrog['pyconfig'] = os.path.join(os.environ['BALROG_MPI'], 'pyconfig', 'slr2.py')
    run['db-columns'] = '/gpfs01/astro/workarea/esuchyta/git-repos/BalrogMPI/sva1_coadd_objects-columns.fits'

    balrog['sexnnw'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.nnw')
    balrog['sexconv'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.conv')
    balrog['sexparam'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.param_diskonly')
    balrog['nosimsexparam'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.param_diskonly')
    balrog['sexconfig'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.config')
    balrog['sexpath'] = os.path.join(os.environ['BALROG_MPI'], 'software','sextractor-2.18.10', 'install-dir','bin','sex')

    return run, balrog


def Y1A1Setup(run, balrog):
    run['release'] = 'y1a1_coadd'
    run['funpack'] = '/gpfs01/astro/workarea/esuchyta/software/cfitsio/install/bin/funpack'
    run['swarp'] = '/gpfs01/astro/workarea/esuchyta/software/swarp-2.36.2/install/bin/swarp'
    run['swarp-config'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_default.swarp'
    run['outdir'] = os.path.join(os.environ['SCRATCH'],'BalrogScratch')

    run['balrog'] = os.path.join(os.environ['BALROG_MPI'], 'software','Balrog','balrog.py')
    balrog['pyconfig'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-pyconfig/fiducial.py'
    run['db-columns'] = '/gpfs01/astro/workarea/esuchyta/git-repos/BalrogMPI/y1a1_coadd_objects-columns.fits'

    balrog['sexnnw'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.nnw'
    balrog['sexconv'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.conv'
    balrog['sexparam'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.param_diskonly'
    balrog['nosimsexparam'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.param_diskonly'
    balrog['sexconfig'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.config'
    balrog['sexpath'] = '/gpfs01/astro/workarea/esuchyta/software/sextractor-2.18.10/install/bin/sex'

    return run, balrog


def CustomConfig(run, balrog, db, tiles):
    run, balrog = Y1A1Setup(run, balrog)
    
    tiles = esutil.io.read('/gpfs01/astro/workarea/esuchyta/git-repos/BalrogDirs/2015-Nov/BalrogMPI/spt-sva1+y1a1-overlap-grizY.fits')
    tiles = tiles['tilename']
    tiles = tiles[100:101]
    
    run['label'] = 'y1a1_test'
    run['joblabel'] = 'test'
    run['nodes'] = 1
    run['ppn'] = 8


    run['tiletotal'] = 50
    balrog['ngal'] = 10

    run['indexstart'] = None
    run['verifyindex'] = True
    run['DBoverwrite'] = False

    return run, balrog, db, tiles

