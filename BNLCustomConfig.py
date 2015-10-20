import os
import numpy as np
import sys
import esutil


# change the defaults if you want
def CustomConfig(run, balrog, db, tiles, where):
    
    # What tiles do you want?  
    #run['release'] = 'y1a1_coadd'
    #tiles = esutil.io.read('y1a1_coadd_spt-grizY-tiles.fits')
    #name = 'DES0356-5331'
    #cut = (tiles['tilename']==name)
    #tiles = tiles[cut]['tilename']

    run['release'] = 'sva1_coadd'
    tiles = tiles[100:106]

    
    # Always check these
    run['label'] = 'db_test'
    run['joblabel'] = '6tiles'
    run['nodes'] = 6
    run['ppn'] = 6


    # If you're not debugging these should be pretty stable not to need to change. 100,000 for the tiletotal gets you to about observed DES number density.
    # Warning: if you make the cleaning parameters False you will use LOTS of disk space
    run['tiletotal'] = 100000
    balrog['ngal'] = 1000
    run['DBoverwrite'] = True
    run['outdir'] = os.path.join(os.environ['SCRATCH'], 'BalrogScratch')
    run['intermediate-clean'] = True
    run['tile-clean'] = True
    run['bands'] = ['g', 'r', 'i', 'z', 'Y']
    run['dualdetection'] = [1,2,3]

    balrog['oldmorph'] = False
    if balrog['oldmorph']:
        balrog["reff"] = "HALF_LIGHT_RADIUS"
        balrog["sersicindex"] = "SERSIC_INDEX"


    return run, balrog, db, tiles

