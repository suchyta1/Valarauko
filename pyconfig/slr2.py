#!/usr/bin/env python

import os
import sys
import numpy as np
import pywcs
import esutil
#import scipy.special
from model_class import *


def CustomArgs(parser):
    parser.add_argument( "-om", "--oldmorph", help="use CMC + morphology catalog", action="store_true")
    
    parser.add_argument( "-reff", "--reff", help="Column name when drawing half light radius from catalog", type=str, default="halflightradius")
    parser.add_argument( "-nsersic", "--sersicindex", help="Column name when drawing sersic index catalog", type=str, default="sersicindex")
    parser.add_argument( "-ax", "--axisratio", help="Axis ratio column", type=str, default="axisratio")
    parser.add_argument( "-beta", "--beta", help="Beta column", type=str, default="beta")

    parser.add_argument( "-tl", "--tile", help="Tilename", type=str, required=True)
    parser.add_argument( "-b", "--band", help="Which filter band to choose from COSMOS catalog. Only relevant if --mag is not given and using COSMOS catlalog.", type=str, default='i', choices=['det','g','r','i','z','Y'])

    parser.add_argument( "-posstart", "--posstart", help="Index to start in position catalog", type=int, default=0)
    parser.add_argument( "-poscat", "--poscat", help="Position catalog", type=str, default=None)
    parser.add_argument( "-posext", "--posext", help="Position extension", type=int, default=1)
    parser.add_argument( "-rakey", "--rakey", help='ra col', type=str, default='ra')
    parser.add_argument( "-deckey", "--deckey", help='dec col', type=str, default='dec')


def ByBand(band, args):
    if band=='det':
        mag = 'det'
    elif band=='Y':
        mag = 'Mapp_HSC_y'
        if args.oldmorph:
            mag = mag.upper()
    else:
        mag = 'Mapp_%s_subaru' %(band)
        if args.oldmorph:
            mag = mag.upper()
    return mag


def CustomParseArgs(args):
    args.ext = 1
    if args.oldmorph:
        args.catalog = os.path.join(os.environ['BALROG_MPI'], 'software', 'Balrog', 'cosmos.fits')
    else:
        #args.catalog = os.path.join(os.environ['BALROG_MPI'], 'catalogs', 'CMC_allband_upsample.fits')
        args.catalog = os.path.join(os.environ['BALROG_MPI'], 'catalogs', 'CMC_allband_upsample_with_blanks.fits')

    args.mag = ByBand(args.band, args)
    if args.ngal > 0:
        coords = np.array( pyfits.open(args.poscat)[args.posext].data[args.posstart:(args.posstart+args.ngal)] )
        args.ra = coords[args.rakey]
        args.dec = coords[args.deckey]


def GetImageCoords(args):
    import pyfits
    header = pyfits.open(args.image)[args.imageext].header
    wcs = pywcs.WCS(header)
    wcoords = np.dstack((args.ra,args.dec))[0]
    pcoords = wcs.wcs_sky2pix(wcoords, 1)
    return pcoords

def GetXCoords(args):
    wcoords = GetImageCoords(args)
    return wcoords[:,0]

def GetYCoords(args):
    wcoords = GetImageCoords(args)
    return wcoords[:,1]


def SLRMag(args, mag):
    sys.path.insert(0, os.environ['BALROG_MPI'])
    import slr_zeropoint_shiftmap as slr
    slrfile = os.path.join(os.environ['BALROG_MPI'], 'slr_zeropoint_shiftmap_v6_splice_cosmos_griz_EQUATORIAL_NSIDE_256_RING.fits')
    slr_map = slr.SLRZeropointShiftmap(slrfile, args.band)
    slr_shift, slr_quality = slr_map.GetZeropointShift(args.band, args.ra, args.dec, mag, interpolate=True) 
    m = mag - slr_shift
    return m


def SimulationRules(args, rules, sampled, TruthCat):
    cat = args.catalog
    ext = args.ext
    tab = Table(file=args.catalog, ext=args.ext)

    rules.x = 0
    rules.y = 0
    rules.g1 = 0
    rules.g2 = 0
    rules.magnification = 1

    rules.halflightradius = tab.Column(args.reff)
    rules.sersicindex = tab.Column(args.sersicindex)
    rules.beta = 0
    rules.axisratio = 1
    rules.magnitude = 30

    if args.ngal > 0:
        rules.x = Function(function=GetXCoords, args=[args])
        rules.y = Function(function=GetYCoords, args=[args])

        TruthCat.AddColumn(args.ra, name='RA', fmt='E')
        TruthCat.AddColumn(args.dec, name='DEC', fmt='E')
        
        if args.band!='det':
            if args.band.upper()=='Y':
                rules.magnitude = tab.Column(args.mag)
            else:
                m = tab.Column(args.mag)
                rules.magnitude = Function(function=SLRMag, args=[args,m])
    else:
        TruthCat.AddColumn(0, name='RA', fmt='E')
        TruthCat.AddColumn(0, name='DEC', fmt='E')
    
    if args.oldmorph:
        TruthCat.AddColumn(tab.Column('ID'), name='id')
        TruthCat.AddColumn(tab.Column('_MOD'), name='mod')
        TruthCat.AddColumn(tab.Column('TYPE'), name='objtype')
        TruthCat.AddColumn(tab.Column('Z'), name='z')
    else:
        rules.beta = tab.Column(args.beta)
        rules.axisratio = tab.Column(args.axisratio)
        TruthCat.AddColumn(tab.Column('Id'), name='id')
        TruthCat.AddColumn(tab.Column('Mod'), name='mod')
        TruthCat.AddColumn(tab.Column('type'), name='objtype')
        TruthCat.AddColumn(tab.Column('z'), name='z')

    TruthCat.AddColumn(args.seed, name='SEED', fmt='J')
    TruthCat.AddColumn(args.indexstart, name='INDEXSTART', fmt='J')
    TruthCat.AddColumn(args.zeropoint, name='ZEROPOINT', fmt='E')

    b = args.mag
    if b=='det':
        b = ByBand('r', args)
    TruthCat.AddColumn(tab.Column(b), name='MAG')



def GalsimParams(args, gsparams, galaxies):
    gsparams.alias_threshold = 1e-3
    gsparams.maximum_fft_size = 12288



def SextractorConfigs(args, config):
    config['CHECKIMAGE_TYPE'] = 'NONE'

    #-CATALOG_TYPE FITS_1.0 
    #-CHECKIMAGE_TYPE SEGMENTATION 
    #-VERBOSE_TYPE NORMAL
    config['WEIGHT_TYPE'] = 'MAP_WEIGHT'
    config['MEMORY_BUFSIZE'] = '2048'
    config['DETECT_THRESH'] = '1.5'
    config['DEBLEND_MINCONT'] = '0.001'


