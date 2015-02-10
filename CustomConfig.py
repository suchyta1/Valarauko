import os

# change the defaults if you want

def CustomConfig(run, balrog, DESdb, db, tiles, where):
    #run['label'] = 'debug_bnl'
    #run['label'] = 'dbg_c'
    #run['DBload'] = 'cx_Oracle'

    run['tiletotal'] = 100000
    #run['tiletotal'] = 2000
    run['DBoverwrite'] = True
    #run['DBload'] = 'sqlldr'
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

    #run['label'] = 'sva1_test'
    #run['joblabel'] = 'test'

    run['label'] = 'sva1v2'
    run['joblabel'] = '0-29'
    run['ppn'] = 6

    run['nodes'] = 10
    tiles = tiles[0:30]

    #run['nodes'] = 2
    #tiles = tiles[0:2]

    return run, balrog, DESdb, db, tiles
