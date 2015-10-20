import os
import copy
import numpy as np

class RunConfigurations:

        default = {
            'joblabel': 'test',
            'nodes': 10,
            'ppn': 6,
            'queue': 'regular', # NERSC queues, irrelevant at BNL
            'walltime': '24:00:00', # irrelevant at BNL
            'module_setup': 'module_setup',

            'funpack': os.path.join(os.environ['BALROG_MPI'], 'software','cfitsio-3.300','funpack'), # this one is the sva1 version, that's not really relevant though
            'swarp': os.path.join(os.environ['BALROG_MPI'], 'software','swarp-2.36.1','install-dir','bin','swarp'), # swarp executable, only relevant if in multi-image detection mode
            'swarp-config': os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'default.swarp'), # swarp configuration file, only relevant if in multi-image detection mode
            'balrog': os.path.join(os.environ['BALROG_MPI'], 'software','Balrog','balrog.py'),  # The Balrog executable you'll use

            'release': 'sva1_coadd',
            'outdir': os.path.join(os.environ['SCRATCH'],'BalrogOutput'),  # The ouput directory for all intermediate work. This should be in the scratch area on the node.
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
            'pyconfig': os.path.join(os.environ['BALROG_MPI'], 'pyconfig', 'slr2.py'),

            'catfitstype': 'ldac',
            'sexnnw': os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.nnw'),
            'sexconv': os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.conv'),
            'sexparam': os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.param_diskonly'),
            'nosimsexparam': os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.param_diskonly'),
            'sexconfig': os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.config'),
            'sexpath': os.path.join(os.environ['BALROG_MPI'], 'software','sextractor-2.18.10', 'install-dir','bin','sex')
        }


class DBInfo:

    default = {
        'db_host' : 'leovip148.ncsa.uiuc.edu',
        'protocol' : 'TCP',
        'port' : '1521',
        'server' : 'dedicated',
        'service_name' : 'dessci',
    }


