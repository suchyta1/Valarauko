import os
import esutil
import imp

dir = os.path.dirname(os.path.dirname(os.path.dirname( os.path.realpath(__file__) )))
BuildJob = imp.load_source('BuildJob', os.path.join(dir,'BuildJob.py'))


def Y1A1Setup(run, balrog, tiles):
    run['release'] = 'y1a1_coadd'
    run['funpack'] = '/gpfs01/astro/workarea/esuchyta/software/cfitsio/install/bin/funpack'
    run['swarp'] = '/gpfs01/astro/workarea/esuchyta/software/swarp-2.36.2/install/bin/swarp'
    run['swarp-config'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_default.swarp'

    run['balrog'] = '/gpfs01/astro/workarea/esuchyta/git-repos/BalrogDirs/2015-Nov/Balrog/balrog.py'
    run['db-columns'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/y1a1_coadd_objects-columns.fits'
    balrog['pyconfig'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-pyconfig/Y1-only.py'

    balrog['sexnnw'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.nnw'
    balrog['sexconv'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.conv'
    balrog['sexparam'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.param_diskonly'
    balrog['nosimsexparam'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.param_diskonly'
    balrog['sexpath'] = '/gpfs01/astro/workarea/esuchyta/software/sextractor-2.18.10/install/bin/sex'
    balrog['sexconfig'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/test_sex.config'
    #balrog['sexconfig'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/20150806_sex.config'

    tiles = esutil.io.read('/gpfs01/astro/workarea/esuchyta/git-repos/BalrogDirs/2015-Nov/BalrogMPI/tiles/spt-y1a1-only-g70-grizY.fits')['tilename']
    run['pos'] = '/gpfs01/astro/workarea/esuchyta/software/Y1A1-config/spt-y1a1-only-g70-grizY-pos-sphere'

    return run, balrog, tiles


def CustomConfig(run, balrog, db, tiles):
    run, balrog, tiles = Y1A1Setup(run, balrog, tiles)
    run['email'] = 'eric.d.suchyta@gmail.com'
    run = BuildJob.TrustEric(run, where='BNL')
    run['ppn'] = 8

    tstart = 0
    tend = 4
    tiles = tiles[tstart:tend]
    run['npersubjob'] = 2
    run['nodes'] = 2
    run['DBoverwrite'] = True

    run['dbname'] = 'y1a1_btest'
    run['joblabel'] = '%i-%i' %(tstart, tend)
    run['outdir'] = os.path.join(os.environ['SCRATCH'],'BalrogScratch')
    run['jobdir'] = os.path.join(os.environ['GLOBALDIR'],'BalrogJobs')

    balrog['ngal'] = 10
    run['downsample'] = balrog['ngal']*run['ppn']
    run['runnum'] = 0 

    return run, balrog, db, tiles
