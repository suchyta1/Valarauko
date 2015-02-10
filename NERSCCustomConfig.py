import os

# change the defaults if you want

def CustomConfig(run, balrog, DESdb, db, tiles, where):

    #run['tiletotal'] = 100000
    run['tiletotal'] = 33000
    run['DBoverwrite'] = True
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

    run['label'] = 'ndbg'
    run['joblabel'] = 'test-third'
    run['ppn'] = 8
    run['walltime'] = '24:00:00'

    #run['nodes'] = 10
    #tiles = tiles[0:30]

    run['nodes'] = 1
    #tiles = tiles[0:2]
    import RunConfigurations
    tiles = RunConfigurations.TileLists.suchyta13[1:2]

    return run, balrog, DESdb, db, tiles
