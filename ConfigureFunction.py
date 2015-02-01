import RunConfigurations
import os
import esutil


# get a default config object
def GetConfig():

    # arguments for configuring the run
    run = RunConfigurations.RunConfigurations.default

    # will get passed as command line arguments to balrog
    balrog = RunConfigurations.BalrogConfigurations.default

    # stuff for talking to the desdb module for finding file
    DESdb = RunConfigurations.desdbInfo.sva1_coadd

    # DB connection info
    db = RunConfigurations.DBInfo.default

    # what files to run balrog over
    tileinfo = esutil.io.read('spte-tiles.fits')
    tiles = tileinfo['tilename']

    run, balrog, DESdb, db, tiles = NerscConfig(run, balrog, desdb, db, tiles)
    #run, balrog, DESdb, db, tiles = BNLConfig(run, balrog, desdb, db, tiles)
    return run, balrog, DESdb, db, tiles


# change the defaults if you want
def NerscConfig(run, balrog, desdb, db, tiles)

    run['label'] = 'debug-nersc'
    run['tiletotal'] = 1000
    run['DBoverwrite'] = True
    run['DBload'] = 'cx_Oracle'
    run['bands'] = ['i']
    run['dualdetection'] = None

    tiles = tiles[0:1]

    return run, balrog, DESdb, db, tiles


    #tiles = cur.quick("SELECT tile.tilename, tile.urall, tile.uraur, tile.udecll, tile.udecur from coaddtile tile   JOIN (select distinct(tilename) as tilename from sva1_coadd_spte) sva1 ON sva1.tilename=tile.tilename  ORDER BY tile.udecll DESC, tile.urall ASC", array=True)


# Runs
# 0-30
