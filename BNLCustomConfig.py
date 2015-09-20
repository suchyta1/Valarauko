import os
import numpy as np
import sys
import esutil


# change the defaults if you want
def CustomConfig(run, balrog, DESdb, db, tiles, where):
    
    # What tiles do you want?  
    lower = 0
    upper = 1
    tiles = esutil.io.read('y1a1_coadd_spt-grizY-tiles.fits')
    tiles = tiles['tilename'][lower:upper]
    DESdb['release'] = 'y1a1_coadd'

    
    # Always check these
    run['label'] = 'y1a1_test'
    run['joblabel'] = '%i-%i'%(lower,upper)
    run['nodes'] = 1
    run['ppn'] = 6


    # If you're not debugging these should be pretty stable not to need to change. 100,000 for the tiletotal gets you to about observed DES number density.
    # Warning: if you make the cleaning parameters False you will use LOTS of disk space
    #run['tiletotal'] = 100000
    run['tiletotal'] = 5000
    run['DBoverwrite'] = True
    run['command'] = 'popen'
    run['DBload'] = 'cx_Oracle'
    run['outdir'] = os.path.join(os.environ['SCRATCH'], 'BalrogScratch')
    run['intermediate-clean'] = True
    run['tile-clean'] = True
    run['bands'] = ['g', 'r', 'i', 'z', 'Y']
    run['dualdetection'] = [1,2,3]

    balrog['oldmorph'] = False
    if balrog['oldmorph']:
        balrog["reff"] = "HALF_LIGHT_RADIUS"
        balrog["sersicindex"] = "SERSIC_INDEX"


    return run, balrog, DESdb, db, tiles

