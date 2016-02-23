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
        thisdir = '/software/Valarauko/source-code'
        astroconfig = os.path.join(thisdir, 'site-setups', 'shifter', 'y1a1', 'astro_config')

        run['release'] = 'y1a1_coadd'
        run['dbcolumn'] = 
