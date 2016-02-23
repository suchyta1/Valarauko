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
        netrc = 'root/.netrc'

        codedir = '/software/Valarauko'
        thisdir = '/software/Valarauko/source-code'

        astroconfig = os.path.join(codedir, 'site-setups', 'shifter', 'y1a1', 'astro_config')
        run['swarp-config'] = os.path.join(astroconfig, '20150806_default.swarp')
        balrog['sexnnw'] = os.path.join(astroconfig, '20150806_sex.nnw')
        balrog['sexconv'] = os.path.join(astroconfig, '20150806_sex.conv')
        balrog['sexparam'] = os.path.join(astroconfig, '20150806_sex.param_diskonly')
        balrog['nosimsexparam'] = os.path.join(astroconfig, '20150806_sex.param_diskonly')
        balrog['sexconfig'] = os.path.join(astroconfig, '20150806_sex.config')

        run['release'] = 'y1a1_coadd'
        balrog['pyconfig'] = os.path.join(dir, 'balrog-config.py')
