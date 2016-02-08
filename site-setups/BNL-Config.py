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

    #run['balrog'] = os.path.join(os.environ['BALROG_MPI'], 'software','Balrog','balrog.py')
    #balrog['pyconfig'] = os.path.join(os.environ['BALROG_MPI'], 'pyconfig', 'slr2.py')
    run['balrog'] = '/gpfs01/astro/workarea/esuchyta/git-repos/BalrogDirs/2015-Nov/Balrog/balrog.py'
    balrog['pyconfig'] = '/gpfs01/astro/workarea/esuchyta/software/SVA1-pyconfig/BalrogConfig-OrigSGQ.py'
    run['db-columns'] = '/gpfs01/astro/workarea/esuchyta/git-repos/BalrogMPI/sva1_coadd_objects-columns.fits'

    balrog['sexnnw'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.nnw')
    balrog['sexconv'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.conv')
    balrog['sexparam'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.param_diskonly')
    balrog['nosimsexparam'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.param_diskonly')
    balrog['sexconfig'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.config')
    balrog['sexpath'] = os.path.join(os.environ['BALROG_MPI'], 'software','sextractor-2.18.10', 'install-dir','bin','sex')

    return run, balrog


def Y1A1Setup(run, balrog, tiles):
    run['release'] = 'y1a1_coadd'
    run['funpack'] = '/gpfs01/astro/workarea/esuchyta/software/cfitsio/install/bin/funpack'
    run['swarp'] = '/gpfs01/astro/workarea/esuchyta/software/swarp-2.36.2/install/bin/swarp'
    run['swarp-config'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_default.swarp'
    run['outdir'] = os.path.join(os.environ['SCRATCH'],'BalrogScratch')

    run['balrog'] = '/gpfs01/astro/workarea/esuchyta/git-repos/BalrogDirs/2015-Nov/Balrog/balrog.py'
    balrog['pyconfig'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-pyconfig/BalrogConfig-OrigSGQ.py'
    run['db-columns'] = '/gpfs01/astro/workarea/esuchyta/git-repos/BalrogMPI/y1a1_coadd_objects-columns.fits'

    balrog['sexnnw'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.nnw'
    balrog['sexconv'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.conv'
    balrog['sexparam'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.param_diskonly'
    balrog['nosimsexparam'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.param_diskonly'
    balrog['sexconfig'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.config'
    balrog['sexpath'] = '/gpfs01/astro/workarea/esuchyta/software/sextractor-2.18.10/install/bin/sex'

    tiles = esutil.io.read('/gpfs01/astro/workarea/esuchyta/git-repos/BalrogDirs/2015-Nov/BalrogMPI/tiles/spt-sva1+y1a1-overlap-grizY.fits')['tilename']
    return run, balrog, tiles


def CustomConfig(run, balrog, db, tiles):
    run, balrog, tiles = Y1A1Setup(run, balrog, tiles)
    run['email'] = 'eric.d.suchyta@gmail.com'

    tstart = 300
    tend = 310
    tiles = tiles[tstart:tend]
    run['joblabel'] = '%i:%i' %(tstart, tend)

    run['nodes'] = 10
    run['ppn'] = 8
    run['label'] = 'y1a1_spto_03'
    run['runnum'] = 0 

    tiletotal = 100000
    run['tiletotal'] = tiletotal
    run['indexstart'] = tstart * tiletotal
    balrog['ngal'] = 1000

    run['DBoverwrite'] = True
    run['verifyindex'] = True

    return run, balrog, db, tiles
