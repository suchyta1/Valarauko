#!/usr/bin/env python

import time
import datetime
import sys
import os
import subprocess
import threading
import Queue
import shlex
import numpy as np
import desdb
import copy
from simple_utils import *
from tilelists import *


class QueueThread(threading.Thread):
    def __init__(self, queue, lock):
        threading.Thread.__init__(self)
        self.queue = queue
        self.lock = lock

    def run(self):
        self._run()

    def _run(self):
        self.lock.acquire()
        while not self.queue.empty():
            job = queue.get()
            self.lock.release()
            subprocess.call(job)
            self.lock.acquire()
        self.lock.release()


def Config2wq(config, index, mincores):
    req = 'mode:bynode; N:1; min_cores:%i; job_name:balrog%i; group:[new,new2,new3]' %(mincores, index)
    cmd = './WrapBalrogOnNode.py'
    for key in config:
        if type(config[key])==bool:
            if config[key]:
                cmd = '%s --%s' %(cmd, key)
        else:
            cmd = '%s --%s %s' %(cmd, key, str(config[key]))
    call = 'wq sub -r "%s" -c "%s"' %(req,cmd)
    args = shlex.split(call)
    return args


def GetRunsTilesDirs(release, withbands, filetype, runkey):
    runs = np.array( desdb.files.get_release_runs(release, withbands=withbands) )
    tiles = np.array([])
    dirs = np.array([])

    kwargs = {}
    kwargs['type'] = filetype

    for run in runs:
        tiles = np.append(tiles, run[-12:])
        kwargs[runkey] = run
        dirs = np.append(dirs, desdb.files.get_dir(**kwargs) )
    return runs, tiles, dirs


def GetRun(tile, alltiles, allruns, alldirs):
    cut = (alltiles==tile)
    run = allruns[cut]
    if len(run)==0:
        raise Exception('Tile %s does not exist' %tile)
    if len(run) > 1:
        raise Exception('Somehow it matched more than one run to the tile %s' %tile)
    return run[0], alldirs[cut][0]


def RunQueue(queue, nodes):
    lock = threading.Lock()
    threads = []
    for i in range(nodes):
        thread = QueueThread(queue, lock)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
        '''
        while thread.isAlive():
            pass
        '''


if __name__ == "__main__":

    release = 'sva1_coadd'
    filetype = 'coadd_image'
    runkey = 'coadd_run'

    maxnodes = 25
    mincores = 8
    bands = ['g', 'r', 'i', 'z', 'Y']
    tiles = TileLists.suchyta13

    #bands = ['r', 'i']
    #tiles = TileLists.suchyta13[0:1]
    #tiles = [TileLists.suchyta13[0], TileLists.suchyta13[12]]

    config = {
        #'pyconfig': os.path.join(os.environ['BALROG_PYCONFIG'], 'default.py'),
        #'sexparam': os.path.join(os.environ['DESDM_CONFIG_SVA1'], 'sex.param_diskonly'),
        #'sexconfig': os.path.join(os.environ['DESDM_CONFIG_SVA1'], 'sex.config'),
        #'presex': False,
        #'ntot': 1000, 

        'pyconfig': os.path.join(os.environ['BALROG_PYCONFIG'], 'r50_r90.py'),
        'label': 'mag_1e2_1n',
        'outdir': os.environ['BALROG_DEFAULT_OUT'],

        'compressed': True,
        'clean': True,
        'fullclean': True,

        'ntot': 300000, 
        'ngal': 1000,
        'kappa': 0.01,

        'presex': True,
        'sexnnw': os.path.join(os.environ['DESDM_CONFIG_SVA1'], 'sex.nnw'),
        'sexconv': os.path.join(os.environ['DESDM_CONFIG_SVA1'], 'sex.conv'),
        'sexpath': '/direct/astro+u/esuchyta/svn_repos/sextractor-2.18.10/install/bin/sex',
        'sexparam': '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/suchyta_config/single_n.param',
        'sexconfig': '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/suchyta_config/r50_r90.config'
    }




    #### Remove DB tables if necessary if the already exist
    user = retrieve_login(db_specs.db_host)[0]
    cur = desdb.connect()
    tns = ['truth', 'nosim', 'sim', 'des']
    band_tables = {}
    for band in bands:
        ts = []
        for tn in tns:
            t = '%s.balrog_%s_%s_%s' %(user, config['label'], tn, band)
            ts.append(t)
        band_tables[band] = ts
    for band in bands:
        for tname in band_tables[band]:
            cur.quick("BEGIN \
                            EXECUTE IMMEDIATE 'DROP TABLE %s'; \
                        EXCEPTION \
                            WHEN OTHERS THEN \
                                IF SQLCODE != -942 THEN \
                                    RAISE; \
                                END IF; \
                        END;" %(tname))
    allruns, alltiles, alldirs = GetRunsTilesDirs(release, bands, filetype, runkey)


    ### Create DBs
    queue_length = len(tiles) * len(bands)
    queue = Queue.Queue(queue_length)
    create_config = copy.copy(config)
    create_config['ntot'] = 0
    create_config['ngal'] = 0
    create_config['create'] = True
    create_config['presex'] = True
    index = 0
    for i in range(len(tiles[:1])):
        create_config['tileindex'] = i
        create_config['tile'] = tiles[i]
        run, dir = GetRun(tiles[i], alltiles, allruns, alldirs)
        create_config['imagedir'] = dir
        for j in range(len(bands)):
            create_config['band'] = bands[j]
            create_config['tables'] = ','.join( band_tables[bands[j]] )
            job = Config2wq(create_config, index, mincores)
            queue.put(job)
            index += 1
    RunQueue(queue, maxnodes)
    
    
    ### Actually run everything
    config['create'] = False
    queue_length = len(tiles) * len(bands)
    queue = Queue.Queue(queue_length)
    index = 0
    for i in range(len(tiles)):
        config['tileindex'] = i
        config['tile'] = tiles[i]
        run, dir = GetRun(tiles[i], alltiles, allruns, alldirs)
        config['imagedir'] = dir
        for j in range(len(bands)):
            config['band'] = bands[j]
            config['tables'] = ','.join( band_tables[bands[j]] )
            job = Config2wq(config, index, mincores)
            queue.put(job)
            index += 1
    RunQueue(queue, maxnodes)
