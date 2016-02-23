#!/usr/bin/env python

import imp
import os
import sys
import esutil
import json
import datetime
import numpy as np
import copy


def GetShifter(version):
    if version=='esuchyta/balrog-docker:v1':
        return Y1A1shifter()


class Y1A1shifter(object):
    def __init__(self, run, balrog):
        runroot = '/Valarauko-job/runroot'
        outroot = '/Valarauko-job/outroot'
        slrroot = '/Valarauko-job/slrroot'
        netrc = 'root/.netrc'
        thisdir = '/software/Valarauko/source-code'

        site = os.path.join(codedir, 'site-setups', 'shifter', 'y1a1')
        balrog['pyconfig'] = os.path.join(site, 'balrog-config.py')

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
