import RunConfigurations
import os
import esutil


# change the defaults if you want
def NerscConfig(run, balrog, DESdb, db, tiles):
    run['label'] = 'debug_nersc'
    run['tiletotal'] = 2000
    run['DBoverwrite'] = True
    run['DBload'] = 'cx_Oracle'
    run['bands'] = ['i']
    run['dualdetection'] = None

    balrog = pyconfig(balrog)
    tiles = tiles[0:2]
    return run, balrog, DESdb, db, tiles


# change the defaults if you want
def BNLConfig(run, balrog, DESdb, db, tiles):
    #run['label'] = 'debug_bnl'
    #run['label'] = 'des_sva1'
    run['label'] = 'sva1_des'
    run['tiletotal'] = 100000
    run['DBoverwrite'] = True
    run['DBload'] = 'cx_Oracle'
    #run['DBload'] = 'sqlldr'

    balrog = pyconfig(balrog)
    tiles = tileinfo['tilename'][0:30]

    return run, balrog, DESdb, db, tiles


def pyconfig(balrog):
    #balrog['sexparam'] = os.path.join(os.environ['BALROG_MPI_ASTRO_CONFIG'], 'sva1', 'sex.param')
    balrog['oldmorph'] = False
    if balrog['oldmorph']:
        balrog["reff"] = "HALF_LIGHT_RADIUS"
        balrog["sersicindex"] = "SERSIC_INDEX"
    return balrog


# get a default config object
def GetConfig():

    # arguments for configuring the run
    run = RunConfigurations.RunConfigurations.default

    # will get passed as command line arguments to balrog
    balrog = RunConfigurations.BalrogConfigurations.default

    # stuff for talking to the DESdb module for finding file
    DESdb = RunConfigurations.desdbInfo.sva1_coadd

    # DB connection info
    db = RunConfigurations.DBInfo.default

    # what files to run balrog over
    tileinfo = esutil.io.read('spte-tiles.fits')
    tiles = tileinfo['tilename']

    #run, balrog, DESdb, db, tiles = NerscConfig(run, balrog, DESdb, db, tiles)
    run, balrog, DESdb, db, tiles = BNLConfig(run, balrog, DESdb, db, tiles)
    return run, balrog, DESdb, db, tiles




    #tiles = cur.quick("SELECT tile.tilename, tile.urall, tile.uraur, tile.udecll, tile.udecur from coaddtile tile   JOIN (select distinct(tilename) as tilename from sva1_coadd_spte) sva1 ON sva1.tilename=tile.tilename  ORDER BY tile.udecll DESC, tile.urall ASC", array=True)


# Runs
# 0-30
