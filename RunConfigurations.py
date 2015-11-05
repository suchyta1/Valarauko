import os
import copy
import numpy as np

class RunConfigurations:

        default = {
            'joblabel': 'test',
            'nodes': 10,
            'ppn': 6,

            # Relevant only for NERSC (only edison is supported in these wrappers)
            'queue': 'regular',
            'walltime': '24:00:00',
            'module_setup': 'sva1_setup',
            'hyper-thread': 1, # 1=usual or 2=hyper-threaded

            'balrog': 'balrog',  # Balrog executable you'll use
            'funpack': 'funpack', # funpack executable 
            'swarp': 'swarp', # swarp executable, only relevant if in multi-image detection mode
            'swarp-config': 'default.swarp', # swarp configuration file, only relevant if in multi-image detection mode

            'release': 'sva1_coadd',
            'db-columns': 'sva1_coadd_objects-columns.fits',

            'outdir': os.path.join(os.path.dirname(os.path.__file__),'BalrogOutput'),  # The ouput directory for all intermediate work. This should be in the scratch area on the node.
            'intermediate-clean': True,  # Delete an iteration's output Balrog images
            'tile-clean': True,  # Delete the entire outdir/run's contents

            'label': 'debug',  # DB tables will look like <username>.balrog_<label>_<type>_<band>
            'DBoverwrite': False,  # Overwrite DB tables with same names (if they exist). False means append into existing tables. Regardless, the tables will be created if they don't exist.

            'tiletotal': 100000, # Approximate number of (truth) Balrog galaxies per tile.
            'fixposseed': None,  # Fix this to an integer to get the same positions every time you run
            'fixwrapseed': None # Fix this to an integer to get the same Balrog sampling realizations each time you run
        }



class BalrogConfigurations:

        default = {
            'fulltraceback': True,
            'ngal': 1000,
            'pyconfig': 'config.py',

            'catfitstype': 'ldac',
            'sexnnw': 'sex.nnw',
            'sexconv': 'sex.conv',
            'sexparam': 'sex.param_diskonly',
            'nosimsexparam': 'sex.param_diskonly',
            'sexconfig': 'sex.config',
            'sexpath': 'sex'
        }


class DBInfo:

    default = {
        'db_host' : 'leovip148.ncsa.uiuc.edu',
        'protocol' : 'TCP',
        'port' : '1521',
        'server' : 'dedicated',
        'service_name' : 'dessci',
    }


