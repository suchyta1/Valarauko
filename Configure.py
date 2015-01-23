import RunConfigurations
import os

def GetConfig():

    # These effect a whole run's behavior. That is they are higher level than a single Balrog call.
    run = RunConfigurations.RunConfigurations.default
    run['label'] = 'debug4'
    run['tiletotal'] = 5000


    # These get passes as command line arguments to Balrog. If you add too much it could mess things up.
    balrog = RunConfigurations.BalrogConfigurations.default
    balrog['pyconfig'] = os.path.join(os.environ['BALROG_MPI_PYCONFIG'], 'slr2.py')


    # This is configuring desdb to find the right files.
    DESdb = RunConfigurations.desdbInfo.sva1_coadd


    # Info for connecting to the DB. You probably don't need to touch this.
    db = RunConfigurations.DBInfo.default


    # What tiles you want to Balrog
    tiles = RunConfigurations.TileLists.suchyta13[1:2]


    return run, balrog, DESdb, db, tiles
