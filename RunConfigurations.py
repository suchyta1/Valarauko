import os
import copy
import numpy as np

class RunConfigurations:

        default = {
            'outdir': os.environ['BALROG_MPI_DEFAULT_OUT'],
            'fullclean': True,
            #'tiletotal': 300000, 
            'tiletotal': 36, 
            'label': 'debug',
            'bands': ['g','r','i','z','Y'],
            'dualdetection': True,
            'doDES': True,
            'balrog': '/astro/u/esuchyta/git_repos/balrog-testing/Balrog/balrog.py'
        }



class BalrogConfigurations:

        default = {
            'fulltraceback': True,
            'clean': True,
            #'ngal': 1000,
            'ngal': 2,
            'pyconfig': os.path.join(os.environ['BALROG_MPI_PYCONFIG'], 'default.py'),
            'magnification': 0.0, 

            'fitstype': 'ldac',
            'sexnnw': os.path.join(os.environ['BALROG_MPI_ASTRO_CONFIG'], 'sva1', 'sex.nnw'),
            'sexconv': os.path.join(os.environ['BALROG_MPI_ASTRO_CONFIG'], 'sva1', 'sex.conv'),
            'sexparam': os.path.join(os.environ['BALROG_MPI_ASTRO_CONFIG'], 'sva1', 'sex.param_diskonly'),
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



