import RunConfigurations
import os
import esutil

def GetConfig():

    # These effect a whole run's behavior. That is they are higher level than a single Balrog call.
    run = RunConfigurations.RunConfigurations.default
    #run['label'] = 'des_sva1'
    #run['tiletotal'] = 100000
    #run['funpack'] = '/astro/u/esuchyta/cfitsio/cfitsio-3.370-install/bin/funpack'
    #run['dualdetection'] = None

    run['label'] = 'debug'
    #run['label'] = 'des_sva1'
    #run['tiletotal'] = 100000
    run['tiletotal'] = 20000
    run['DBoverwrite'] = True
    #run['DBload'] = 'cx_Oracle'
    run['DBload'] = 'sqlldr'
    run['intermediate-clean'] = True
    run['tile-clean'] = True


    # These get passes as command line arguments to Balrog. If you add too much it could mess things up.
    balrog = RunConfigurations.BalrogConfigurations.default
    #balrog['sexparam'] = os.path.join(os.environ['BALROG_MPI_ASTRO_CONFIG'], 'sva1', 'sex.param')
    balrog['oldmorph'] = False
    if balrog['oldmorph']:
        balrog["reff"] = "HALF_LIGHT_RADIUS"
        balrog["sersicindex"] = "SERSIC_INDEX"


    # What tiles you want to Balrog
    #tiles = RunConfigurations.TileLists.suchyta13
    tileinfo = esutil.io.read('spte-tiles.fits')
    #tiles = tileinfo['tilename'][0:30]
    tiles = tileinfo['tilename'][0:1]



    # This is configuring desdb to find the right files.
    DESdb = RunConfigurations.desdbInfo.sva1_coadd


    # Info for connecting to the DB. You probably don't need to touch this.
    db = RunConfigurations.DBInfo.default




    #tiles = cur.quick("SELECT tile.tilename, tile.urall, tile.uraur, tile.udecll, tile.udecur from coaddtile tile   JOIN (select distinct(tilename) as tilename from sva1_coadd_spte) sva1 ON sva1.tilename=tile.tilename  ORDER BY tile.udecll DESC, tile.urall ASC", array=True)
    return run, balrog, DESdb, db, tiles


# Runs
# 0-30
