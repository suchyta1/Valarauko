import os


def SVA1Setup(run, balrog):
    run['release'] = 'sva1_coadd'
    run['module_setup'] = 'balrog_sva1_setup'
    run['swarp-config'] = os.path.join(os.environ['ASTRO_CONFIG'], 'default.swarp') 
    run['outdir'] = os.path.join(os.environ['SCRATCH'],'BalrogOutput')
    run['balrog'] = '/scratch1/scratchdirs/esuchyta/software/Balrog/balrog.py'

    balrog['pyconfig'] = os.path.join(os.environ['BALROG_MPI'], 'pyconfig', 'slr2.py')
    balrog['sexnnw'] = os.path.join(os.environ['ASTRO_CONFIG'],'sex.nnw')
    balrog['sexconv'] = os.path.join(os.environ['ASTRO_CONFIG'], 'sex.conv')
    balrog['sexparam'] = os.path.join(os.environ['ASTRO_CONFIG'], 'sex.param_diskonly')
    balrog['nosimsexparam'] = os.path.join(os.environ['ASTRO_CONFIG'], 'sex.param_diskonly')
    balrog['sexconfig'] = os.path.join(os.environ['ASTRO_CONFIG'], 'sex.config')

    return run


# change the defaults if you want
def CustomConfig(run, balrog, db, tiles):

    # Always check these
    run = SVA1Setup(run, balrog)
    run['command'] = 'system'

    run['label'] = 'system2'
    run['joblabel'] = 'large'
    run['ppn'] = 24
    run['nodes'] = 10
    run['walltime'] = '15:00:00'
    run['queue'] = 'regular'
    tiles = tiles[30:40]


    # If you're not debugging these should be pretty stable not to need to change. 100,000 for the tiletotal gets you to about observed DES number density.
    # Warning: if you make the cleaning parameters False you will use LOTS of disk space
    run['tiletotal'] = 100000
    balrog['ngal'] = 1000

    run['DBoverwrite'] = True
    run['outdir'] = os.path.join(os.environ['SCRATCH'], 'BalrogScratch')
    run['intermediate-clean'] = True
    run['tile-clean'] = True

    balrog['oldmorph'] = False
    if balrog['oldmorph']:
        balrog["reff"] = "HALF_LIGHT_RADIUS"
        balrog["sersicindex"] = "SERSIC_INDEX"


    return run, balrog, db, tiles
