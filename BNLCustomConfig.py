import os

# change the defaults if you want

def CustomConfig(run, balrog, DESdb, db, tiles, where):

    # Always check these
    run['label'] = 'sva1v2'
    run['joblabel'] = '30-49'
    run['ppn'] = 6
    run['nodes'] = 10
    tiles = tiles[30:50]


    # If you're not debugging these should be pretty stable not to need to change. 100,000 for the tiletotal gets you to about observed DES number density.
    # Warning: if you make the cleaning parameters False you will use LOTS of disk space
    run['tiletotal'] = 100000
    run['DBoverwrite'] = False
    run['DBload'] = 'cx_Oracle'
    run['inc'] = 100
    run['outdir'] = os.path.join(os.environ['SCRATCH'], 'BalrogScratch')
    run['intermediate-clean'] = True
    run['tile-clean'] = True
    run['bands'] = ['g', 'r', 'i', 'z', 'Y']
    run['dualdetection'] = [1,2,3]
    balrog['oldmorph'] = False


    if balrog['oldmorph']:
        balrog["reff"] = "HALF_LIGHT_RADIUS"
        balrog["sersicindex"] = "SERSIC_INDEX"


    '''
    run['label'] = 'debug'
    run['joblabel'] = 'test'
    run['ppn'] = 8
    run['nodes'] = 2
    tiles = tiles[30:32]

    run['tiletotal'] = 5000
    run['DBoverwrite'] = True
    run['DBload'] = 'cx_Oracle'
    run['inc'] = 100
    run['outdir'] = os.path.join(os.environ['SCRATCH'], 'BalrogScratch')
    run['intermediate-clean'] = False
    run['tile-clean'] = False
    run['bands'] = ['g', 'r', 'i', 'z', 'Y']
    run['dualdetection'] = [1,2,3]
    balrog['oldmorph'] = True


    if balrog['oldmorph']:
        balrog["reff"] = "HALF_LIGHT_RADIUS"
        balrog["sersicindex"] = "SERSIC_INDEX"
    '''

    return run, balrog, DESdb, db, tiles
