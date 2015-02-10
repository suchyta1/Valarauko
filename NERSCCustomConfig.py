import os

# change the defaults if you want

def CustomConfig(run, balrog, DESdb, db, tiles, where):
    #run['label'] = 'debug_bnl'
    #run['label'] = 'dbg_c'
    #run['DBload'] = 'cx_Oracle'

    #run['tiletotal'] = 100000
    run['tiletotal'] = 7015
    run['DBoverwrite'] = True
    #run['DBload'] = 'sqlldr'
    run['DBload'] = 'cx_Oracle'
    run['inc'] = 10
    run['outdir'] = os.path.join(os.environ['SCRATCH'], 'BalrogScratch')
    run['intermediate-clean'] = True
    run['tile-clean'] = True

    run['bands'] = ['r', 'i', 'z']
    run['dualdetection'] = [0,1,2]

    balrog['oldmorph'] = True
    if balrog['oldmorph']:
        balrog["reff"] = "HALF_LIGHT_RADIUS"
        balrog["sersicindex"] = "SERSIC_INDEX"

    #run['label'] = 'sva1_test'
    #run['joblabel'] = 'test'

    run['label'] = 'ndbg'
    run['joblabel'] = 'test-regular2'
    run['ppn'] = 8
    run['walltime'] = '01:00:00'

    #run['nodes'] = 10
    #tiles = tiles[0:30]

    run['nodes'] = 2
    tiles = tiles[0:2]

    return run, balrog, DESdb, db, tiles
