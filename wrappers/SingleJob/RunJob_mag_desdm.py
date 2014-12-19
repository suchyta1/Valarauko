#!/usr/bin/env python

import sys
import os
import Queue
import numpy as np
import desdb
import copy
import pywcs
import pyfits
import itertools
from mpi4py import MPI

from simple_utils import *
from runconfigs import *
import RunNode



def GetRunsTilesDirs(withbands, SheldonConfig):
    runs = np.array( desdb.files.get_release_runs(SheldonConfig['release'], withbands=withbands) )
    tiles = np.array([])
    dirs = np.array([])

    kwargs = {}
    kwargs['type'] = SheldonConfig['filetype']

    for run in runs:
        tiles = np.append(tiles, run[-12:])
        kwargs[SheldonConfig['runkey']] = run
        dirs = np.append(dirs, desdb.files.get_dir(**kwargs) )
    return [runs, tiles, dirs]


def GetRun(tile, runinfo):
    allruns, alltiles, alldirs = runinfo
    cut = (alltiles==tile)
    run = allruns[cut]
    if len(run)==0:
        raise Exception('Tile %s does not exist' %tile)
    if len(run) > 1:
        raise Exception('Somehow it matched more than one run to the tile %s' %tile)
    return run[0], alldirs[cut][0]



def InitializeTables(config, bands):
    band_tables = None
    if MPI.COMM_WORLD.Get_rank()==0:
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
    band_tables = MPI.COMM_WORLD.bcast(band_tables, root=0)
    return band_tables


def GetTilesWCS(tiles, runinfo):

    wcs = []
    rmin = []
    rmax = []
    dmin = []
    dmax = []
    pcoords = np.dstack( (np.array([0,0,10000,10000]),np.array([0,10000,10000,0])) )[0]

    tc = np.empty( (0,2), dtype='f8,f8')
    for i in range(len(tiles)):
        run, dir = GetRun(tiles[i], runinfo)

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

    return [wcs, ramin, ramax, decmin, decmax]


def Broadcast(arr):
    for i in range(len(arr)):
        arr[i] = MPI.COMM_WORLD.bcast(arr[i], root=0)
    return arr


def GetRunInfo(bands, SheldonConfig):
    info = [None]*3
    if MPI.COMM_WORLD.Get_rank() == 0:
        info = GetRunsTilesDirs(bands, SheldonConfig)
    info = Broadcast(info) 
    return info


def GetFileInfo(tiles, runinfo):
    wcsinfo = [None]*5
    if MPI.COMM_WORLD.Get_rank() == 0:
        wcsinfo = GetTilesWCS(tiles, runinfo)
    wcsinfo = Broadcast(wcsinfo)
    return wcsinfo


def GeneratePositions(config, fileinfo, seed=None):
    wcs, ramin, ramax, decmin, decmax = fileinfo
    tcoords = []
    wtcoords = []
    target = len(wcs) * config['ntot'] / float(MPI.COMM_WORLD.size)
    #inc = 10000 
    inc = 200
    numfound = 0
    iterations = 0
    
    if seed!=None:
        np.random.seed(seed)

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

            wcoordlist = wcoords[cut]
            coordlist = pcoords[cut]

            if iterations==0:
                tcoords.append(coordlist)
                wtcoords.append(wcoordlist)
            else:
                tcoords[i] = np.concatenate((tcoords[i], coordlist), axis=0)
                wtcoords[i] = np.concatenate((wtcoords[i], wcoordlist), axis=0)
            ind = index[cut]
            notin = -np.in1d(ind, found)
            found = np.append(found, ind[notin])
        iterations += 1
        numfound += len(found)

    tilecoords = []
    wtilecoords = []
    for i in range(len(tcoords)):
        t = MPI.COMM_WORLD.allgather(tcoords[i])
        t = np.array( list(itertools.chain.from_iterable(t)) )
        tilecoords.append(t)
    for i in range(len(wtcoords)):
        w = MPI.COMM_WORLD.allgather(wtcoords[i])
        w = np.array( list(itertools.chain.from_iterable(w)) )
        wtilecoords.append(w)

    tilecoords = np.array(tilecoords)
    wtilecoords = np.array(wtilecoords)
    return tilecoords, wtilecoords, [ramin, decmin]



def ServeQueue(queue):
    size = MPI.COMM_WORLD.size - 1
    done = 0
    while done < size:
        rank_recv = MPI.COMM_WORLD.recv(source=MPI.ANY_SOURCE)
        if rank_recv == -1:
            done += 1
        else:
            if queue.qsize() > 0:
                nextjob = queue.get()
            else:
                nextjob = -1
            MPI.COMM_WORLD.send(nextjob, dest=rank_recv)

def SlaveQueue():
    rank = MPI.COMM_WORLD.Get_rank()
    while True:
        job = MPI.COMM_WORLD.sendrecv(rank, dest=0, source=0)
        if job==-1:
            MPI.COMM_WORLD.send(-1, dest=0)
            break
        else:
            RunNode.RunBalrog(*job)


def TileConfig(tile, runinfo, galcount, coords, wcoords):
    tile_config = {}
    run, dir = GetRun(tile, runinfo)
    tile_config['imagedir'] = dir
    tile_config['tile'] = tile
    tile_config['indexstart'] = galcount
    tile_config['tilecoords'] = coords
    tile_config['wcoords'] = wcoords
    galcount += len(tile_config['tilecoords'])
    return tile_config, galcount

def BandConfig(band, band_tables):
    band_config = {}
    band_config['band'] = band
    band_config['tnames'] = band_tables[band]
    return band_config

def MakeQueue(bands, tiles, runinfo, coords, wcoords, band_tables, config):
    size = len(bands) * len(tiles)
    tbqueue = Queue.Queue(size)
    galcount = 0
    for i in range(len(tiles)):
        tile_config, galcount = TileConfig(tiles[i], runinfo, galcount, coords[i], wcoords[i])
        for j in range(len(bands)):
            band_config = BandConfig(bands[j], band_tables)
            tbqueue.put( [config, tile_config, band_config] )
    return tbqueue



def BuildCreateQueue(config, bands, tiles, runinfo, band_tables, wmin):
    emptycoords = [ []*len(bands) ]
    create_config = copy.copy(config)
    create_config['ngal'] = 0
    create_config['create'] = True
    create_config['presex'] = True
    create_config['ramin'] = wmin[0]
    create_config['decmin'] = wmin[1]
    ts = tiles[:1]
    tbqueue = MakeQueue(bands, ts, runinfo, emptycoords, emptycoords, band_tables, create_config)
    return tbqueue


def BuildFullQueue(config, bands, tiles, runinfo, band_tables, tilecoords, wcoords, wmin):
    config['create'] = False
    config['ramin'] = wmin[0]
    config['decmin'] = wmin[1]
    tbqueue = MakeQueue(bands, tiles, runinfo, tilecoords, wcoords, band_tables, config)
    return tbqueue


def CreateEmptyTables(config, bands, tiles, runinfo, band_tables, wmin):
    rank = MPI.COMM_WORLD.Get_rank()
    if rank==0:
        tbqueue = BuildCreateQueue(config, bands, tiles, runinfo, band_tables, wmin)
        server = ServeQueue(tbqueue)
    if rank > 0:
        SlaveQueue()


def DoBalrog(config, bands, tiles, runinfo, band_tables, tilecoords, wcoords, wmin):
    rank = MPI.COMM_WORLD.Get_rank()
    if rank==0:
        tbqueue = BuildFullQueue(config, bands, tiles, runinfo, band_tables, tilecoords, wcoords, wmin)
        server = ServeQueue(tbqueue)
    if rank > 0:
        SlaveQueue()


if __name__ == "__main__":
    #bands = ['g', 'r', 'i', 'z', 'Y']
    #bands = ['i']
    #bands = ['r']
    #bands = ['z']
    #bands = ['g']
    bands = ['Y']
    SheldonConfig = SheldonInfo.sva1_coadd
    tiles = TileLists.suchyta13
    config = Configurations.mag_desdm

    runinfo = GetRunInfo(bands, SheldonConfig)
    fileinfo = GetFileInfo(tiles, runinfo)
    tilecoords, wcoords, wmin = GeneratePositions(config, fileinfo, seed=100)
    band_tables = InitializeTables(config, bands)


    CreateEmptyTables(config, bands, tiles, runinfo, band_tables, wmin)
    MPI.COMM_WORLD.barrier()

    DoBalrog(config, bands, tiles, runinfo, band_tables, tilecoords, wcoords, wmin)

