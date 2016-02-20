#!/usr/bin/env python

import os
import numpy as np
import argparse
import copy
import shutil
import desdb
import esutil
import fitsio
import numpy.lib.recfunctions as rec
import suchyta_utils as es
import suchyta_utils.mpi as mpi
from mpi4py import MPI


def PosCat(ra, dec):
    pos = np.empty(len(ra), dtype=[('ra',np.float32), ('dec',np.float32)])
    pos['ra'] = ra
    pos['dec'] = dec
    return pos

def GetHeader(args):
    s = args.seed
    if s is None:
        s = 'None'
    header = {'samp':args.sampling, 'pertile': args.pertile, 'seed': s, 'itby': args.iterateby}
    return header

def GetPos(num, it, args):
    if args.seed is not None:
        np.random.seed(args.seed + it)

    ra, dec = es.balrog.UniformRandom(num, ramin=0, ramax=360, decmin=-90, decmax=90)
    pos = PosCat(ra, dec)
    return pos

def FindInTile(pos, ra1, ra2, dec1, dec2):
    cut = (pos['ra'] > ra1) & (pos['ra'] < ra2) & (pos['dec'] > dec1) & (pos['dec'] < dec2)
    return pos[cut]

def GetTileDefs(args, strtype='|S12'):
    #t = esutil.io.read(args.tiles)[args.tilecol][0:2]
    t = esutil.io.read(args.tiles)[args.tilecol]
    tindex = np.arange(len(t))
    tiles = np.empty(len(t), dtype=[('tilename',strtype), ('index', np.int64)])
    tiles['tilename'] = t.astype(strtype)
    tiles['index'] = np.arange(len(t))

    if args.sampling=='sphere':
        for tile in tiles['tilename']:
            outdir = os.path.join(args.outdir, tile)
            if not os.path.exists(outdir):
                os.makedirs(outdir)

    cur = desdb.connect()
    q = "select urall, uraur, udecll, udecur, tilename from coaddtile order by udecll desc, urall asc"
    arr = cur.quick(q, array=True)

    dt = arr.dtype.descr
    dt[-1] = ('tilename',strtype)
    dt = np.dtype(dt)
    newarr = np.empty(len(arr), dtype=dt)
    for i in range(len(arr.dtype.names)):
        name = arr.dtype.names[i]
        if i == 4:
            newarr[name] = arr[name].astype(strtype)
        else:
            newarr[name] = arr[name]

    tiles = rec.join_by('tilename', newarr, tiles, usemask=False)
    tiles = np.sort(tiles, order='index')
    return tiles


def GetArgs():
    parser = argparse.ArgumentParser()

    # These affect the random number generations, so would have to be set identically to a previous run to get the same results.
    parser.add_argument("-sa", "--sampling", help="How to sample: equal number per tile, or realizations over full sphere", default='sphere', type=str, choices=['sphere','tile'])
    parser.add_argument("-p", "--pertile", help="(Average) number per tile", default=100000, type=int)
    parser.add_argument("-s", "--seed", help="Seed for random number generator, so you can get the same positions again if you add to your tile list", default=None, type=int)
    parser.add_argument("-i", "--iterateby", help="Only relevant with --sampling sphere. Number of sphere points per random realization iteratition. Reducing lessens memory consumption at cost of run time.", default=int(1e8), type=int)

    # The tile list affects the indexstart that gets written. If you give a tile list that has extra tiles appended compared to before, the new indexstarts will be consistent before what was appended.
    parser.add_argument("-t", "--tiles", help="FITS file with tile list", required=True, type=str)

    parser.add_argument("-tc", "--tilecol", help="Column in FITS file with tile names", default='tilename', type=str)
    parser.add_argument("-o", "--outdir", help="Output directory to save position files", default=None, type=str)

    args = parser.parse_args()


    if args.outdir is None:
        args.outdir = os.path.join( os.path.dirname(args.tiles), '%s-tilepos'%(os.path.basename(args.tiles).rstrip('.fits')) )
    args.outdir = os.path.realpath(args.outdir)

    if MPI.COMM_WORLD.Get_rank()==0:
        if not os.path.exists(args.outdir):
            os.makedirs(args.outdir)
    
    return args


def SetupIterations(args):
    density = args.pertile / np.power(1.0e4*0.27*np.pi/(3600.0*180.0), 2)
    total = int( density * 4.0*np.pi )
    fullits = total / args.iterateby
    num = np.array( [total/fullits]*fullits )
    it = np.arange(len(num))
    leftover = total % args.iterateby
    if (leftover > 0):
        num = np.append(num, leftover)
        it = np.append(it, it[-1]+1)
    return num, it



def CatFiles(args, tilecopy, itcopy):
    for tile in tilecopy['tilename']:
        started = True
        for i in range(len(itcopy)):
            f = os.path.join(args.outdir, tile, 'tmp-%i.fits'%(itcopy[i]))
            if not started:
                try:
                    pos = esutil.io.read(f)
                    started = True
                except IOError:
                    continue
            else:
                try:
                    new = esutil.io.read(f)
                except IOError:
                    continue
                pos = rec.stack_arrays( (pos,new), usemask=False )
        shutil.rmtree(os.path.join(args.outdir, tile))    
        outfile = os.path.join(args.outdir, '%s.fits'%(tile))
        header = GetHeader(args)
        esutil.io.write(outfile, pos, clobber=True, header=header)
        print 'wrote %s'%(outfile)
    MPI.COMM_WORLD.barrier()

def AddIndexstart(tiles):
    indexstart = 0
    for tile in tiles['tilename']:
        outfile = os.path.join(args.outdir, '%s.fits'%(tile))
        fits = fitsio.FITS(outfile, 'rw')
        fits[-1].write_key('istart', indexstart)
        indexstart += fits[-1].read_header()['NAXIS2']
        fits.close()

def WriteTmp(args, num, it, tiles):
    for i in range(len(num)):
        pos = GetPos(num[i], it[i], args)
        for j in range(len(tiles['tilename'])):
            tilepos = FindInTile(pos, tiles[j]['urall'],tiles[j]['uraur'], tiles[j]['udecll'],tiles[j]['udecur'])
            outfile = os.path.join(args.outdir, tiles[j]['tilename'], 'tmp-%i.fits'%(it[i]))
            header = GetHeader(args)
            esutil.io.write(outfile, tilepos, clobber=True)
    MPI.COMM_WORLD.barrier()


def EqualGen(args, tiles):
    for i  in range(len(tiles)):
        ra, dec = es.balrog.UniformRandom(args.pertile, ramin=tiles[i]['urall'], ramax=tiles[i]['uraur'], decmin=tiles[i]['udecll'], decmax=tiles[i]['udecur'])
        pos = PosCat(ra, dec)
        header = GetHeader(args)
        outfile = os.path.join(args.outdir, '%s.fits'%(tiles[i]['tilename']))
        esutil.io.write(outfile, pos, clobber=True, header=header)
        print 'wrote %s'%(outfile)
    MPI.COMM_WORLD.barrier()


if __name__ == "__main__":
    args = GetArgs() 

    if MPI.COMM_WORLD.Get_rank()==0:
        tiles = GetTileDefs(args)
        tilecopy = copy.deepcopy(tiles)
    else:
        tiles = None

      
    if args.sampling=='sphere':
        if MPI.COMM_WORLD.Get_rank()==0:
            num, it = SetupIterations(args)
            itcopy = copy.deepcopy(it)
        else:
            num = it = itcopy = None

        tiles, itcopy = mpi.Broadcast(tiles, itcopy)
        num, it = mpi.Scatter(num, it)
        WriteTmp(args, num, it, tiles)
        
        tiles = mpi.Scatter(tiles)
        CatFiles(args, tiles, itcopy)

    else:
        tiles = mpi.Scatter(tiles)
        EqualGen(args, tiles)


    if MPI.COMM_WORLD.Get_rank()==0:
        AddIndexstart(tilecopy)
