import os
import numpy as np
import sys

# change the defaults if you want

def CustomConfig(run, balrog, DESdb, db, tiles, where):
    
    """
    run['label'] = 'cosmos_v1'
    run['joblabel'] = 'cosmos_v1_run'
    run['nodes'] = 13
    run['ppn'] = 6
    tiles = np.loadtxt('cosmos-tiles.txt', dtype=np.str)
    """

    # Always check these
    run['label'] = 'sva1v3_b'
    run['joblabel'] = '380-394'
    run['nodes'] = 15
    run['ppn'] = 6
    tiles = tiles[380:395]

    #cut = (tiles=='DES0559-6039')
    #tiles = tiles[cut]
   
    """
    badtiles1 = np.array( ['DES0407-5540', 'DES0407-5622', 'DES0408-5705', 'DES0410-5831',
           'DES0412-5540', 'DES0413-5622', 'DES0413-5705', 'DES0414-5748',
           'DES0416-5831', 'DES0417-5540', 'DES0418-5622', 'DES0419-5705',
           'DES0420-5748', 'DES0421-5831', 'DES0422-5540', 'DES0423-5622',
           'DES0424-5705', 'DES0425-5748', 'DES0427-5540', 'DES0427-5831',
           'DES0428-5622', 'DES0429-5705', 'DES0431-5748', 'DES0432-5831',
           'DES0434-5705', 'DES0436-5748', 'DES0437-5831', 'DES0440-5705',
           'DES0441-5748', 'DES0443-5831', 'DES0445-5705', 'DES0447-5748',
           'DES0448-5831', 'DES0450-5705', 'DES0452-5748', 'DES0454-5622',
           'DES0454-5831', 'DES0456-5705', 'DES0457-5748', 'DES0459-5622'] )

    badtiles2 = np.array( ['DES0459-5831', 'DES0501-5705', 'DES0502-5540', 'DES0503-5748',
           'DES0504-5622', 'DES0506-5705', 'DES0508-5540', 'DES0508-5748',
           'DES0509-5622', 'DES0511-5457', 'DES0511-5705', 'DES0513-5540',
           'DES0513-5748', 'DES0514-5622', 'DES0516-5457', 'DES0517-5705',
           'DES0518-5540', 'DES0519-5748', 'DES0520-5622', 'DES0521-5457',
           'DES0522-5705', 'DES0523-5540', 'DES0524-5748', 'DES0525-5622',
           'DES0526-5457', 'DES0527-5705', 'DES0528-5540', 'DES0529-5748',
           'DES0530-5622', 'DES0532-5705', 'DES0535-5622', 'DES0535-5748',
           'DES0538-5705', 'DES0540-5622', 'DES0540-5748', 'DES0543-5705',
           'DES0546-5748'] )

    badtiles3= np.array( ['DES0527-5705'] )

    tiles = badtiles3
    """

    '''
    run['label'] = 'sva1v6'
    run['joblabel'] = '3x3'
    tiles = np.array( ['DES0436-4831', 'DES0440-4831', 'DES0445-4831', 'DES0436-4914', 'DES0441-4914', 'DES0445-4914', 'DES0436-4957', 'DES0441-4957', 'DES0445-4957'] )
    run['tiletotal'] = 500000
    #run['DBoverwrite'] = True
    '''


    # If you're not debugging these should be pretty stable not to need to change. 100,000 for the tiletotal gets you to about observed DES number density.
    # Warning: if you make the cleaning parameters False you will use LOTS of disk space
    run['tiletotal'] = 100000
    #run['DBoverwrite'] = False
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

