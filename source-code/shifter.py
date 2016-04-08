import os

class Y1A1shifter(object):

    def __init__(self, run, balrog):
        self.jobroot = '/Valarauko-job/jobroot'
        self.outroot = '/Valarauko-job/outroot'
        self.slrroot = '/Valarauko-job/slrroot'
        self.catroot = '/Valarauko-job/catroot'
        self.posroot = '/Valarauko-job/posroot'
        self.homeroot = '/home/user/site'

        gitdir = '/software/Valarauko'
        self.thisdir = os.path.join(gitdir, 'source-code')
        site = os.path.join(gitdir, 'site-setups', 'shifter', 'y1a1')
        #balrog['pyconfig'] = os.path.join(site, 'balrog-config.py')
        balrog['pyconfig'] = os.path.join(gitdir, 'pyconfig', 'y1a1.py')

        astroconfig = os.path.join(site, 'astro_config')
        run['swarp-config'] = os.path.join(astroconfig, '20150806_default.swarp')
        balrog['sexnnw'] = os.path.join(astroconfig, '20150806_sex.nnw')
        balrog['sexconv'] = os.path.join(astroconfig, '20150806_sex.conv')
        balrog['sexparam'] = os.path.join(astroconfig, '20150806_sex.param_diskonly')
        balrog['nosimsexparam'] = os.path.join(astroconfig, '20150806_sex.param_diskonly')
        balrog['sexconfig'] = os.path.join(astroconfig, '20150806_sex.config')

        other = os.path.join(site, 'other')
        run['release'] = 'y1a1_coadd'
        run['db-columns'] = os.path.join(other, 'y1a1_coadd_objects-columns.fits')


def GetShifter(run, balrog):
    if run['shifter']=='esuchyta/balrog-docker:v1':
        return Y1A1shifter(run, balrog)
    elif run['shifter'].endswith('y1a1'):
        return Y1A1shifter(run, balrog)
