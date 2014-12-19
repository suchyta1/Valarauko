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
import pywcs
import pyfits
import astropy.cosmology as cosmology
import astropy.units as units
import scipy.interpolate as interpolate

import matplotlib.pyplot as plt
from simple_utils import *
#from tilelists import *
from runconfigs import *


def SetPower(kappa_fft, freq, kscale=0.1):
    kx, ky, kz = np.meshgrid(freq[0], freq[1], freq[2], indexing='ij')

    #zero = (kz==0)
    #kzed = np.zeros(zero.shape)
    #kzed[-zero] = np.power(10, -1.0/kz[-zero])
    #print kzed

    k2 = kx*kx + ky*ky + kz*kz
    #k2 = kx*kx + ky*ky + kzed*kzed
    
    phase = np.angle(kappa_fft)
    amp2 = np.exp( -k2/(kscale*kscale) )
    kappa_fft = np.sqrt(amp2) * np.exp(1j*phase)
    return kappa_fft


def GetKappaField(dims, delta, kscale=0.1, amp_scale=0.01):
    kappa = np.random.normal(loc=0, scale=1.0, size=dims)
    kappa_fft = np.fft.fftn(kappa, axes=(-3,-2,-1))
    freq = []
    for i in range(len(kappa.shape)):
        freq.append(np.fft.fftfreq(kappa.shape[i], d=delta[i]))
        #freq.append(np.fft.fftfreq(kappa.shape[i]))
    freq = np.array(freq)
    kappa_fft = SetPower(kappa_fft, freq, kscale)
    kappa = np.fft.ifftn(kappa_fft, axes=(-3,-2,-1))
    sig = np.std(np.real(kappa))
    return kappa/sig


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

def angle(r1=0, r2=0, d1=0, d2=0):
    rdiff = np.abs(r2-r1)
    n1 = np.cos(d2) * np.sin(rdiff)
    n2 = np.cos(d1) * np.sin(d2)
    n3 = np.sin(d1) * np.cos(d2) * np.cos(rdiff)
    n = np.power(n1, 2) + np.power(n2-n3, 2)
    num = np.sqrt(n)
    d1 = np.sin(d1) * np.sin(d2)
    d2 = np.cos(d1) * np.cos(d2) * np.cos(rdiff)
    den = d1 + d2
    ang = np.arctan(num / den)
    ang = np.arctan2(den, num)
    return ang


def MakeField():
    cosmo = cosmology.Planck13

    zfile = '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/input_cats/CMC_sersic.fits'
    redshifts = pyfits.open(zfile)[1].data['z']
    zmin = np.amin(redshifts)
    zmax = np.amax(redshifts)

    print zmin, zmax
    print (1+zmax) * cosmo.angular_diameter_distance(zmax).value

    plt.figure(1)
    reds = np.arange(zmin, zmax, step=0.05)
    s = 1.5 * np.pi / 180.0 * cosmo.angular_diameter_distance(reds).value
    plt.plot(reds, s)

    plt.figure(2)
    plt.hist(redshifts, bins=20)
    plt.show()

    #sys.exit()

    dimx = 1024
    dimy = 256
    dimz = 1024
    k_scale = 0.01
    amp_scale = 0.01
  
    zmin = 0.005
    zmax = 6.0

    zlim = np.array([zmin,zmax])
    zlim = (1+zlim) * cosmo.angular_diameter_distance(zlim).value
    #zlim = np.log10(zlim)
    deltaz = (zlim[1] - zlim[0]) / (dimz-1)
    index = np.arange(dimz)
    #sys.exit()

    dims = (dimx, dimy, dimz)
    delta = np.array([1.0, 1.0, deltaz]) # in Mpc, Mpc, log
    kappa = GetKappaField(dims, delta, kscale=k_scale, amp_scale=amp_scale)
    
    slice = 0
    kr = np.real(kappa)
    x = np.arange(dims[0])
    y = np.arange(dims[1])
    #z = np.power(10, zlim[0]+delta[2]*np.arange(dims[2]))
    z = np.arange(dims[2])*deltaz

    coords = np.meshgrid(x, y, z, indexing='ij')
    #cs = np.dstack( (coords[0],coords[1],coords[2]) )[0]
    #x,y,z = np.meshgrid(x, y, z, indexing='ij')
    #r = np.sqrt(x*x+y*y+z*z)*units.Mpc

    rr = np.random.uniform(0, 0.5)
    rd = np.random.uniform(0, 0.5)
    rz = np.random.uniform(0, 0.2)
    ang = angle(r2=rr, d2=rd)
    r = cosmo.angular_diameter_distance(rz).value

    zp = r * np.cos(ang)
    xp = r * np.sin(ang) * np.cos(rd)
    yp = r * np.sin(ang) * np.cos(rr)
    print xp, yp, zp
    print rr, rd, rz, ang

    #xp = np.random.uniform( [0,dimx] )
    #yp = np.random.uniform( [0,dimy] )
    #zp = cosmo.angular_diameter_distance( np.random.uniform( [zmin,zmax] ) )
    

    xdiff = np.power(x-xp, 2)
    xd = np.column_stack( (x,xdiff) )
    xclose = np.sort(xd.view('f8,f8'), order=['f1'], axis=0)[:2, 0]
    xi = [xclose[0][0], xclose[1][0]]
    xd = [xclose[0][1], xclose[1][1]]

    ydiff = np.power(y-yp, 2)
    yd = np.column_stack( (y,ydiff) )
    yclose = np.sort(yd.view('f8,f8'), order=['f1'], axis=0)[:2, 0]
    yi = [yclose[0][0], yclose[1][0]]
    yd = [yclose[0][1], yclose[1][1]]
    
    zdiff = np.power(z-zp, 2)
    zind = np.arange(len(z))
    zd = np.column_stack( (zind,zdiff) )
    zclose = np.sort(zd.view('f8,f8'), order=['f1'], axis=0)[:2, 0]
    zi = [zclose[0][0], zclose[1][0]]
    zd = [zclose[0][1], zclose[1][1]]

    kap = []
    w = []
    for i in range(len(xi)):
        for j in range(len(yi)):
            for k in range(len(zi)):
                kk = np.real(kappa[xi[i], yi[j], zi[k]])
                kap.append(kk)
                dist = np.sqrt( xd[i] + yd[j] + zd[k] )
                w.append(dist)

    k = np.average(kap, weights=w)

    '''
    cs = np.column_stack( (coords[0].flatten(), coords[1].flatten(), coords[2].flatten()) )
    index = np.arange(len(cs))
    ds = np.sum( np.power( cs - np.array( [100,100,100] ), 2), axis=-1 )
    print ds.shape, coords[0].shape
    cd = np.column_stack( (coords[0].flatten(),coords[1].flatten(),coords[2].flatten(), ds) )
    sort = np.sort( cd.view('f8,f8,f8,f8'), order=['f3'], axis=0 )
    closest = sort[0:8]
    print closest
    '''

    #print cs.shape, kappa.flatten().shape
    #interpolater = interpolate.LinearNDInterpolator(cs, kappa.flatten())
    #print interpolater(100,100, 100)
    

    #redshift = np.ones(z.shape)
    #dist = cosmo.comoving_distance(redshift.flatten()).value
    #dist = dist.reshape(redshift.shape)
    #print dist

    '''
    for i in range(len(r)):
        print i
        for j in range(len(r[i])):
            for k in range(len(r[i][j])):
                redshift[i][j][k] = cosmology.z_at_value(cosmo.comoving_distance, r[i][j][k])
    '''

    #print redshift

    fig = plt.figure(1, figsize=(10,6))
    ax = fig.add_subplot(2,2,1)
    cax = ax.imshow(kr[:,:,slice].transpose(), origin='lower', cmap=plt.get_cmap('jet'))
    cbar = fig.colorbar(cax)
    #ax.set_aspect(float(dimx)/dimy)

    ax = fig.add_subplot(2,2,2)
    cax = ax.imshow(kr[:,:,(slice+1)].transpose(), origin='lower', cmap=plt.get_cmap('jet'))
    cbar = fig.colorbar(cax)

    ax = fig.add_subplot(2,2,3)
    cax = ax.imshow(kr[:,:,(slice+2)].transpose(), origin='lower', cmap=plt.get_cmap('jet'))
    cbar = fig.colorbar(cax)

    ax = fig.add_subplot(2,2,4)
    cax = ax.imshow(kr[:,:,(slice+3)].transpose(), origin='lower', cmap=plt.get_cmap('jet'))
    cbar = fig.colorbar(cax)

    print z[slice], z[slice+1], z[slice+2], z[slice+3]
    plt.tight_layout()
    plt.show()


def GetArea():

    release = 'sva1_coadd'
    filetype = 'coadd_image'
    runkey = 'coadd_run'

    maxnodes = 25
    mincores = 8
    bands = ['g', 'r', 'i', 'z', 'Y']
    tiles = TileLists.suchyta27

    config = {
        'pyconfig': os.path.join(os.environ['BALROG_PYCONFIG'], 'r50_r90_coords.py'),
        'label': 'mag_10sqdeg',
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
    '''
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
    '''
    allruns, alltiles, alldirs = GetRunsTilesDirs(release, bands, filetype, runkey)


    wcs = []
    rmin = []
    rmax = []
    dmin = []
    dmax = []
    pcoords = np.dstack( (np.array([0,0,10000,10000]),np.array([0,10000,10000,0])) )[0]

    #tiles = tiles[0:3]
    tcoords = []
    tc = np.empty( (0,2), dtype='f8,f8')
    for i in range(len(tiles)):
        run, dir = GetRun(tiles[i], alltiles, allruns, alldirs)

        filename = os.path.join(dir, '%s_i.fits.fz'%(tiles[i]))
        hdu = pyfits.open(filename)[1]
        header = hdu.header
        w = pywcs.WCS(header)
        wcs.append(w)
        wcoords = w.wcs_pix2sky(pcoords, 1)
        rmin.append( np.amin(wcoords[:,0]) )
        rmax.append( np.amax(wcoords[:,0]) )
        dmin.append( np.amin(wcoords[:,1]) )
        dmax.append( np.amax(wcoords[:,1]) )
   
    ramin = min(rmin)
    ramax = max(rmax)
    decmin = min(dmin) * np.pi / 180.0
    if decmin < 0:
        decmin = np.pi/2.0 - decmin
    decmax = max(dmax) * np.pi / 180.0
    if decmax < 0:
        decmax = np.pi/2.0 - decmax
    print ramin, ramax
    print decmin, decmax

    target = len(wcs) * config['ntot']
    inc = 10000 
    numfound = 0
    iterations = 0
    while numfound < target:
        found = np.empty(0)

        ra = np.random.uniform(ramin,ramax, inc)
        dec = np.arccos( np.random.uniform(np.cos(decmin),np.cos(decmax), inc) ) * 180.0 / np.pi
        neg = (dec > 90.0)
        dec[neg] = 90.0 - dec[neg]
        wcoords = np.dstack((ra,dec))[0]
        index = np.arange(inc)

        for i in range(len(wcs)):
            w = wcs[i]
            pcoords = w.wcs_sky2pix(wcoords, 1) - 0.5
            cut = (pcoords[:,0] > 0) & (pcoords[:,0] < 10000) & (pcoords[:,1] > 0) & (pcoords[:,1] < 10000)
            coordlist = wcoords[cut]
            if iterations==0:
                tcoords.append(coordlist)
            else:
                tcoords[i] = np.concatenate((tcoords[i], coordlist), axis=0)
            ind = index[cut]
            notin = -np.in1d(ind, found)
            found = np.append(found, ind[notin])
        iterations += 1
        numfound += len(found)

    print numfound, iterations*inc
    print len(tcoords[0]), len(tcoords[1]), len(tcoords[2])


    """
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
    """

if __name__ == "__main__":

    MakeField()
    #GetArea()
    
