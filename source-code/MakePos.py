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
from mpi4py import MPI


def GetPos(num, it[i], args):
    if args.seed is not None:
        np.random.seed(args.seed + it[i])

    ra, dec = es.db.UniformRandom(num, ramin=0, ramax=360, decmin=-90, decmax=90)
    pos = np.empty(num, dtype=[('ra',np.float32), ('dec',np.float32)])
    pos['ra'] = ra
    pos['dec'] = dec
    return pos


def FindInTile(pos, ra1, ra2, dec1, dec2):
    cut = (pos['ra'] > ra1) & (pos['ra'] < ra2) & (pos['dec'] > dec1) & (pos['dec'] < dec2)
    return pos[cut]

def GetTileDef(args):
    t = esutil.io.read(args.tiles)[args.tilecol]
    tindex = np.arange(len(t))
    tiles = np.empty(len(t), dtype=[('tilename','|S12'), ('index', np.int64)])
    tiles['tilename'] = t
    tiles['index'] = np.arange(len(t))

    cur = desdb.connect()
    q = "select urall, uraur, udecll, udecur, tilename from coaddtile order by udecll desc, urall asc"
    arr = cur.quick(q, array=True)

    #index = np.arange(len(tiles))
    #cut = np.in1d(arr['tilename'], tiles)

    tiles = rec.join_by('tilename', arr, tiles, usemask=False)
    tiles = np.sort(tiles, order='index')

    return tiles[cut]


def GetArgs():
    parser = argparse.ArgumentParser()

    # These affect the random number generations, so would have to be set identically to a previous run to get the same results.
    parser.add_argument("-d", "--density", help="Object density, in deg^(-2)", default=200000., type=float)
    parser.add_argument("-s", "--seed", help="Seed for random number generator, so you can get the same positions again if you add to your tile list", default=None, type=int)
    parser.add_argument("-i", "--iterateby", help="Number of sphere points per random realization iteratition. Reducing lessens memory consumption at cost of run time.", default=int(5e7), type=int)

    # The tile list affects the indexstart that gets written. If you give a tile list that has extra tiles appended compared to before, the new indexstarts will be consistent before what was appended.
    parser.add_argument("-t", "--tiles", help="FITS file with tile list", required=True, type=str)

    parser.add_argument("-tc", "--tilecol", help="Column in FITS file with tile names", default='tilename', type=str)
    parser.add_argument("-o", "--outdir", help="Output directory to save position files", default=None, type=str)

    args = parser.parse_args()


    if args.outdir is None:
        f = os.path.realpath(args.tiles)
        args.outdir = os.path.join(os.path.dirname(f), '%s-tilepos'%(os.path.basename(args.tiles).rstrip('.fits')) )

    if MPI.COMM_WORLD()==0:
        if not os.path.exists(args.outdir):
            os.makedirs(args.outdir)
        for tile in args.tiles:
            outdir = os.path.join(args.outdir, tile)
            if not os.path.exist(outdir):
                os.makedirs(outdir)
    
    return args


def SetupIterations(args):
    total = int( args.density * 4.0*np.pi * np.power(180./np.pi, 2) )
    fullits = total / args.iterateby
    num = np.array( [total/fullits]*fullits )
    it = np.arange(num)
    leftover = total % args.iterateby
    if (leftover > 0):
        num = np.append(num, leftover)
        it = np.append(it, it[0]+1)
    return num, it


def CatFiles(args, tilecopy, itcopy):
    indexstart = 0
    for tile in tilecopy['tilename']:
        for i in range(len(itcopy)):
            f = os.path.join(args.outdir, tile, 'tmp-%i.fits'%(itcopy[i]))
            if i == 0:
                pos = esutil.io.read(f)
            else:
                new = esutil.io.read(f)
                pos = rec.stack_arrays( (pos,new), usemask=False )
        shutil.rmtree(os.path.join(args.outdir, tile))    
        outfile = os.path.join(args.outdir, '%s.fits'%(tile))
        header = {'density': args.density, 'seed': args.seed, 'iterateby': args.iterateby, 'indexstart': indexstart}
        esutil.io.write(outfile, pos, clobber=True, header=header)
        indexstart += len(pos)
        print 'wrote %s'%(outfile)


def WriteTmp(args, num, it, tiles):
    for i in range(len(num)):
        pos = GetPos(num[i], it[i], args)
        for j in range(len(tiles['tilename'])):
            tilepos = FindInTile(pos, tiles[j]['urall'],tiles[j]['uraur'], tiles[j]['udecll'],tiles[j]['udecur'])
            outfile = os.path.join(args.outdir, tiles[j]['tilename'], 'tmp-%i.fits'%(it[i]))
            esutil.io.write(outfile, tilepos, clobber=True)


if __name__ == "__main__":
    args = GetArgs() 

    if MPI.COMM_WORLD().Get_rank()==0:
        num, it = SetupIterations()
        tiles = GetTileDefs(args)
        itcopy = copy.deepcopy(it)
        tilecopy = copy.deepcopy(tiles)
    else:
        num = None
        tiles = None
   
    tiles = es.mpi.Broadcast(tiles)
    num, it = es.mpi.Scatter(num, it)
    WriteTmp(args, num, it, tiles)

    MPI.COMM_WORLD.barrier()
    if MPI.COMM_WORLD.Get_rank()==0:
        CatFiles(args, tilecopy, itcopy):
