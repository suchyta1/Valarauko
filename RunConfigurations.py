import os
import copy
import numpy as np

class RunConfigurations:

        default = {
            'nomulti': False,  # don't use the mulitprocessing module, useful for debugging
            'funpack': '/astro/u/esuchyta/cfitsio/cfit3300-install/bin/funpack', # this one is the sva1 version, that's not really relevant though
            'swarp': '/astro/u/esuchyta/svn_repos/swarp-2.36.1/install/bin/swarp', # swarp executable, only relevant if in multi-image detection mode
            'swarp-config': os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'default.swarp'), # swarp configuration file, only relevant if in multi-image detection mode

            'balrog': '/astro/u/esuchyta/git_repos/balrog-testing/Balrog/balrog.py',  # The Balrog executable you'll use
            'outdir': os.path.join(os.environ['SCRATCH'],'BalrogOutput'),  # The ouput directory for all intermediate work. This should be in the scratch area on the node.
            'intermediate-clean': True,  # Delete an iteration's output Balrog images
            'tile-clean': True,  # Delete the entire outdir/run's contents

            'label': 'debug',  # DB tables will look like <username>.balrog_<label>_<type>_<band>
            'DBload': 'cx_Oracle',  # ['cx_Oracle', 'sqlldr'] How to write to DB. 
            'DBoverwrite': False,  # Overwrite DB tables with same names (if they exist). False means append into existing tables. Regardless, the tables will be created if they don't exist.

            'tiletotal': 100000, # Approximate number of (truth) Balrog galaxies per tile.
            'fixposseed': None,  # Fix this to an integer to get the same positions every time you run
            'fixwrapseed': None, # Fix this to an integer to get the same Balrog sampling realizations each time you run

            'doDES': False,  # Run sextractor without any Balrog galaxies over full images
            'bands': ['g','r','i','z','Y'], # Bands you'll get measurement catalogs for
            'dualdetection': [1,2,3]  # Use None not to use detection image. Otherwise the indices in the array of bands.

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
            'sexconfig': os.path.join(os.environ['BALROG_MPI'], 'astro_config', 'sva1', 'sex.config'),
            'sexpath': os.path.join(os.environ['BALROG_MPI'], 'software','sextractor-2.18.10', 'install-dir','bin','sex')
        }


class desdbInfo:

    sva1_coadd = {
        'release': 'sva1_coadd',
        'filetype': 'coadd_image',
        'runkey': 'coadd_run',
    }


class DBInfo:

    default = {
        'db_host' : 'leovip148.ncsa.uiuc.edu',
        'protocol' : 'TCP',
        'port' : '1521',
        'server' : 'dedicated',
        'service_name' : 'dessci',
    }


class TileLists:

    suchyta13 = ['DES0415-4831',
                 'DES0419-4831',
                 'DES0423-4831',
                 'DES0427-4831',
                 'DES0432-4831',
                 'DES0436-4831',
                 'DES0440-4831',
                 'DES0445-4831',
                 'DES0449-4831',
                 'DES0453-4831',
                 'DES0458-4831',
                 'DES0502-4831',
                 'DES0506-4831']

    suchyta14 = ['DES0411-4748',
                 'DES0415-4748',
                 'DES0419-4748',
                 'DES0423-4748',
                 'DES0428-4748',
                 'DES0432-4748',
                 'DES0436-4748',
                 'DES0440-4748',
                 'DES0445-4748',
                 'DES0449-4748',
                 'DES0453-4748',
                 'DES0457-4748',
                 'DES0502-4748',
                 'DES0506-4748']

    spte3 = ['DES0411-4706',
             'DES0415-4706',
             'DES0419-4706',
             'DES0424-4706',
             'DES0428-4706',
             'DES0432-4706',
             'DES0436-4706',
             'DES0440-4706',
             'DES0445-4706',
             'DES0449-4706',
             'DES0453-4706',
             'DES0457-4706',
             'DES0501-4706',
             'DES0506-4706']

    suchyta27 = np.append( np.array(suchyta13), np.array(suchyta14) )



