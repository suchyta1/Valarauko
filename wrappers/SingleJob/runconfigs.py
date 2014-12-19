import os
import copy
import numpy as np

class Configurations:
        mag_10sqdeg = {
            'pyconfig': os.path.join(os.environ['BALROG_PYCONFIG'], 'r50_r90_coords.py'),
            #'label': 'mag_10sqdeg',
            'outdir': os.environ['BALROG_DEFAULT_OUT'],
            'magimage':'/astro/u/esuchyta/git_repos/BalrogSetupBNL/wrappers/SingleJob/magfield.fits',

            'compressed': True,
            'clean': True,
            'fullclean': True,

            'ntot': 300000, 
            #'ntot': 1000, 
            'ngal': 1000,
            
            #'nmin': str(0.3),
            #'nmax': str(6.0),
            #'dn': str(0.1),
            #'ntype': 'lin',
            #'label': 'nobin',

            'presex': True,
            'fitstype': 'ldac',
            'sexnnw': os.path.join(os.environ['DESDM_CONFIG_SVA1'], 'sex.nnw'),
            'sexconv': os.path.join(os.environ['DESDM_CONFIG_SVA1'], 'sex.conv'),
            'sexpath': '/direct/astro+u/esuchyta/svn_repos/sextractor-2.18.10/install/bin/sex',
            'sexparam': '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/suchyta_config/single_n.param',
            'sexconfig': '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/suchyta_config/r50_r90.config'
        }

        mag_desdm = {
            'pyconfig': os.path.join(os.environ['BALROG_PYCONFIG'], 'mag_desdm.py'),
            'outdir': os.environ['BALROG_DEFAULT_OUT'],

            'compressed': True,
            'clean': True,
            'fullclean': True,

            'ntot': 300000, 
            'ngal': 1000,

            #'label': 'nomag_desdm',
            #'magnification': 0.0, 

            'label': 'mag_desdm',
            'magnification': 0.01, 

            'presex': True,
            'fitstype': 'ldac',
            'sexnnw': os.path.join(os.environ['DESDM_CONFIG_SVA1'], 'sex.nnw'),
            'sexconv': os.path.join(os.environ['DESDM_CONFIG_SVA1'], 'sex.conv'),
            'sexpath': '/direct/astro+u/esuchyta/svn_repos/sextractor-2.18.10/install/bin/sex',

            'sexparam': '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/DESDM_config/sva1/sex.param_diskonly',
            'sexconfig': '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/DESDM_config/sva1/sex.config'
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

    suchyta27 = np.append( np.array(suchyta13), np.array(suchyta14) )



class SheldonInfo:
    sva1_coadd = {
        'release': 'sva1_coadd',
        'filetype': 'coadd_image',
        'runkey': 'coadd_run',
    }
