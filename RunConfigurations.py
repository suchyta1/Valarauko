import os
import copy
import numpy as np

class RunConfigurations:

        default = {
            'balrog': '/astro/u/esuchyta/git_repos/balrog-testing/Balrog/balrog.py',
            'outdir': os.environ['BALROG_MPI_DEFAULT_OUT'],
            'intermediate-clean': True,
            'tile-clean': True, 

            'label': 'debug8',
            'DBload': 'cx_Oracle',

            #'tiletotal': 300000, 
            'tiletotal': 50000, 
            #'tiletotal': 1000, 
            'fixposseed': None,
            'fixwrapseed': None,

            'doDES': True,
            #'doDES': False,
            'bands': ['g','r','i','z','Y'],
            'dualdetection': [1,2,3]
            #'bands': ['det'],
            #'dualdetection': None
            #'bands': ['r','i','z'],
            #'dualdetection': [0,1,2]

        }



class BalrogConfigurations:

        default = {
            'fulltraceback': True,
            'ngal': 1000,
            #'ngal': 50,
            #'pyconfig': os.path.join(os.environ['BALROG_MPI_PYCONFIG'], 'default.py'),
            'pyconfig': os.path.join(os.environ['BALROG_MPI_PYCONFIG'], 'lessdefault.py'),

            'catfitstype': 'ldac',
            'sexnnw': os.path.join(os.environ['BALROG_MPI_ASTRO_CONFIG'], 'sva1', 'sex.nnw'),
            'sexconv': os.path.join(os.environ['BALROG_MPI_ASTRO_CONFIG'], 'sva1', 'sex.conv'),
            #'sexparam': os.path.join(os.environ['BALROG_MPI_ASTRO_CONFIG'], 'sva1', 'sex.param_diskonly'),
            'sexparam': os.path.join(os.environ['BALROG_MPI_ASTRO_CONFIG'], 'sva1', 'sex.param'),
            'sexconfig': os.path.join(os.environ['BALROG_MPI_ASTRO_CONFIG'], 'sva1', 'sex.config'),
            'sexpath': '/direct/astro+u/esuchyta/svn_repos/sextractor-2.18.10/install/bin/sex',
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



