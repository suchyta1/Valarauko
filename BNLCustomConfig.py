import os
import numpy as np
import sys
import esutil


def SVA1Setup(run, balrog):
    run['release'] = 'sva1_coadd'
    run['funpack'] = os.path.join(os.environ['BALROG_MPI'], 'software','cfitsio-3.300','funpack')
    run['swarp'] = os.path.join(os.environ['BALROG_MPI'], 'software','swarp-2.36.1','install-dir','bin','swarp')
    run['swarp-config'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'default.swarp')
    run['balrog'] = os.path.join(os.environ['BALROG_MPI'], 'software','Balrog','balrog.py')
    run['outdir'] = os.path.join(os.environ['SCRATCH'],'BalrogOutput')

    balrog['pyconfig'] = os.path.join(os.environ['BALROG_MPI'], 'pyconfig', 'slr2.py')
    balrog['sexnnw'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.nnw')
    balrog['sexconv'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.conv')
    balrog['sexparam'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.param_diskonly')
    balrog['nosimsexparam'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.param_diskonly')
    balrog['sexconfig'] = os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.config')
    balrog['sexpath'] = os.path.join(os.environ['BALROG_MPI'], 'software','sextractor-2.18.10', 'install-dir','bin','sex')

    return run, balrog


# change the defaults if you want
def CustomConfig(run, balrog, db, tiles):
    
    # What tiles do you want?  
    #run['release'] = 'y1a1_coadd'
    #tiles = esutil.io.read('y1a1_coadd_spt-grizY-tiles.fits')
    #name = 'DES0356-5331'
    #cut = (tiles['tilename']==name)
    #tiles = tiles[cut]['tilename']

    tiles = tiles[100:106]

    
    # Always check these
    run, balrog = SVA1Setup(run, balrog)
    run['label'] = 'db_test'
    run['joblabel'] = '6tiles'
    run['nodes'] = 6
    run['ppn'] = 6


    # If you're not debugging these should be pretty stable not to need to change. 100,000 for the tiletotal gets you to about observed DES number density.
    # Warning: if you make the cleaning parameters False you will use LOTS of disk space
    run['tiletotal'] = 50
    balrog['ngal'] = 10
    run['DBoverwrite'] = True
    run['outdir'] = os.path.join(os.environ['SCRATCH'], 'BalrogScratch')
    run['intermediate-clean'] = True
    run['tile-clean'] = True

    balrog['oldmorph'] = False
    if balrog['oldmorph']:
        balrog["reff"] = "HALF_LIGHT_RADIUS"
        balrog["sersicindex"] = "SERSIC_INDEX"


    return run, balrog, db, tiles

