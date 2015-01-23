#!/usr/bin/env python

import os
import sys
import numpy as np
import pywcs
import esutil
import scipy.special
from model_class import *

usemorph = True


def CustomArgs(parser):
    parser.add_argument( "-cs", "--catalog", help="Catalog used to sample simulated galaxy parameter distriubtions from", type=str, default=None)
    parser.add_argument( "-ext", "--ext", help="Index of the data extension for sampling catalog", type=int, default=1)
    
    if usemorph:
        parser.add_argument( "-reff", "--reff", help="Column name when drawing half light radius from catalog", type=str, default="halflightradius")
        parser.add_argument( "-nsersic", "--sersicindex", help="Column name when drawing sersic index catalog", type=str, default="sersicindex")
        parser.add_argument( "-ax", "--axisratio", help="Axis ratio column", type=str, default="axisratio")
        parser.add_argument( "-beta", "--beta", help="Beta column", type=str, default="beta")
    else:
        parser.add_argument( "-reff", "--reff", help="Column name when drawing half light radius from catalog", type=str, default="HALF_LIGHT_RADIUS")
        parser.add_argument( "-nsersic", "--sersicindex", help="Column name when drawing sersic index catalog", type=str, default="SERSIC_INDEX")

    parser.add_argument( "-tl", "--tile", help="Tilename", type=str, required=True)
    parser.add_argument( "-b", "--band", help="Which filter band to choose from COSMOS catalog. Only relevant if --mag is not given and using COSMOS catlalog.", type=str, default='i', choices=['det','g','r','i','z','Y'])

    parser.add_argument( "-posstart", "--posstart", help="Index to start in position catalog", type=int, default=0)
    parser.add_argument( "-poscat", "--poscat", help="Position catalog", type=str, default=None)
    parser.add_argument( "-posext", "--posext", help="Position extension", type=int, default=1)
    parser.add_argument( "-rakey", "--rakey", help='ra col', type=str, default='ra')
    parser.add_argument( "-deckey", "--deckey", help='dec col', type=str, default='dec')


def ByBand(band):
    if band=='det':
        mag = 'det'
    elif band=='Y':
        mag = 'Mapp_HSC_y'
        if not usemorph:
            mag = mag.upper()
    else:
        mag = 'Mapp_%s_subaru' %(band)
        if not usemorph:
            mag = mag.upper()
    return mag


def CustomParseArgs(args):
    thisdir = os.path.dirname( os.path.realpath(__file__) )
    if args.catalog==None:
        if usemorph:
            args.catalog = '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/input_cats/CMC_allband_upsample.fits'
        else:
            args.catalog = '/astro/u/esuchyta/git_repos/balrog-testing/Balrog/cosmos.fits'

    args.mag = ByBand(args.band)

    if args.ngal > 0:
        coords = np.array( pyfits.open(args.poscat)[args.posext].data[args.posstart:(args.posstart+args.ngal)] )
        args.ra = coords[args.rakey]
        args.dec = coords[args.deckey]
    else:
        args.ra = []
        args.dec = []


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
    sys.path.insert(0, '/astro/u/esuchyta/git_repos/BalrogMPI/')
    import slr_zeropoint_shiftmap as slr
    slrfile = '/astro/u/esuchyta/git_repos/BalrogMPI/slr_zeropoint_shiftmap_v6_splice_cosmos_griz_EQUATORIAL_NSIDE_256_RING.fits'
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
        
        if args.band!='det':
            if args.band.upper()=='Y':
                rules.magnitude = tab.Column(args.mag)
            else:
                m = tab.Column(args.mag)
                rules.magnitude = Function(function=SLRMag, args=[args,m])
    
    if usemorph:
        rules.beta = tab.Column(args.beta)
        rules.axisratio = tab.Column(args.axisratio)
        TruthCat.AddColumn(tab.Column('Id'))
        TruthCat.AddColumn(tab.Column('Mod'))
        TruthCat.AddColumn(tab.Column('type'), name='OBJTYPE')
        TruthCat.AddColumn(tab.Column('z'))
    else:
        TruthCat.AddColumn(tab.Column('ID'), name='CMCID')
        TruthCat.AddColumn(tab.Column('_MOD'), name='CMCMOD')
        TruthCat.AddColumn(tab.Column('TYPE'), name='CMCTYPE')
        TruthCat.AddColumn(tab.Column('Z'))

    TruthCat.AddColumn(args.seed, name='SEED', fmt='J')
    TruthCat.AddColumn(args.zeropoint, name='ZEROPOINT', fmt='E')
    TruthCat.AddColumn(args.ra, name='RA', fmt='E')
    TruthCat.AddColumn(args.dec, name='DEC', fmt='E')

    b = args.mag
    if b=='det':
        b = ByBand('r')
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


