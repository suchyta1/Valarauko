#!/usr/bin/env python

import os
import sys
import numpy as np
import pywcs
from model_class import *


def CustomArgs(parser):
    parser.add_argument( "-cs", "--catalog", help="Catalog used to sample simulated galaxy parameter distriubtions from", type=str, default=None)
    parser.add_argument( "-ext", "--ext", help="Index of the data extension for sampling catalog", type=int, default=1)
    parser.add_argument( "-reff", "--reff", help="Column name when drawing half light radius from catalog", type=str, default="halflightradius")
    parser.add_argument( "-nsersic", "--sersicindex", help="Column name when drawing sersic index catalog", type=str, default="sersicindex")
    parser.add_argument( "-ax", "--axisratio", help="Axis ratio column", type=str, default="axisratio")
    parser.add_argument( "-beta", "--beta", help="Beta column", type=str, default="beta")

    parser.add_argument( "-tl", "--tile", help="Tilename", type=str, required=True)
    
    parser.add_argument( "-b", "--band", help="Which filter band to choose from COSMOS catalog. Only relevant if --mag is not given and using COSMOS catlalog.", type=str, default='i', choices=['det','g','r','i','z','Y'])
    parser.add_argument( "-detb", "--detbands", help="detection bands", type=str, default='r,i,z')
    parser.add_argument( "-detz", "--detzeropoints", help="zeropoints for detection bands", type=str, default=None)

    parser.add_argument( "-posstart", "--posstart", help="Index to start in position catalog", type=int, default=0)
    parser.add_argument( "-poscat", "--poscat", help="Position catalog", type=str, default=None)
    parser.add_argument( "-posext", "--posext", help="Position extension", type=int, default=1)
    parser.add_argument( "-rakey", "--rakey", help='ra col', type=str, default='ra')
    parser.add_argument( "-deckey", "--deckey", help='dec col', type=str, default='dec')


def ByBand(band):
    if band=='det':
        mag = 'det'
    if band=='Y':
        mag = 'Mapp_HSC_y'
    else:
        mag = 'Mapp_%s_subaru' %(band)
    return mag


def CustomParseArgs(args):
    thisdir = os.path.dirname( os.path.realpath(__file__) )
    if args.catalog==None:
        args.catalog = '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/input_cats/CMC_allband_upsample.fits'

    if args.band == 'det':
        if args.detbands==None:
            raise Exception("when using --band = 'det', you must also give --detbands")
        if args.detzeropoints==None:
            raise Exception("when using --band = 'det', you must also give --detzeropoints")
    if args.detbands!=None:
        args.detbands = args.detbands.split(',')
        args.detzeropoints = args.detzeropoints.split(',')
        for i in range(len(args.detzeropoints)):
            args.detzeropoints[i] = float(args.detzeropoints[i])
    args.mag = ByBand(args.band)

    if args.ngal > 0:
        coords = np.array( pyfits.open(args.poscat)[args.posext].data[args.posstart:(args.posstart+args.ngal)] )
        args.ra = coords[args.rakey]
        args.dec = coords[args.deckey]
    else:
        args.ra = []
        args.dec = []


def GetImageCoords(args):
    header = pyfits.open(args.image)[args.imageext].header
    wcs = pywcs.WCS(header)
    wcoords = np.dstack((args.ra,args.dec))[0]
    wcoords = w.wcs_sky2pix(wcoords, 1)

def GetXCoords(args, wpos):
    wcoords = GetImageCoords(args, wpos)
    return wcoords[:,0]

def GetYCoords(args, wpos):
    wcoords = GetImageCoords(args, wpos)
    return wcoords[:,1]

def MultibandMag(mz, args):
    flux = np.zeros(args.ngal)
    for i in range(len(mz)):
        mag, zp = mz[i]
        f = np.power(10.0, (zp - mag) / 2.5)
        flux += f
    mag = Flux2Mag(flux, args)
    return mag

def Flux2Mag(flux, args):
    mag = np.array( [99.0]*args.ngal )
    cut = (flux > 0)
    args.zeropoint - 2.5 * np.log10(flux)
    mag[cut] = args.zeropoint - 2.5 * np.log10(flux)
    return mag


def SimulationRules(args, rules, sampled, TruthCat):
    cat = args.catalog
    ext = args.ext
    tab = Table(file=args.catalog, ext=args.ext)

    rules.g1 = 0
    rules.g2 = 0
    rules.magnification = 0

    if args.ngal > 0:
        rules.x = Function(function=GetXCoords, args=[args])
        rules.y = Function(function=GetYCoords, args=[args])
    else:
        rules.x = 0
        rules.y = 0
    
    rules.halflightradius = tab.Column(args.reff)
    rules.beta = tab.Column(args.beta)
    rules.axisratio = tab.Column(args.axisratio)
    rules.sersicindex = tab.Column(args.sersicindex)

    if args.band=='det':
        bz = []
        for i in range(len(args.detbands)):
            m = ByBand(args.detbands[i])
            z = args.detzeropoints[i]
            bz.append( [tab.Column(m),z] )
        rules.magnitude = Function(function=MultibandMag, args=[bz,args])
    else:
        rules.magnitude = tab.Column(args.mag)


    TruthCat.AddColumn(tab.Column('Id'))
    TruthCat.AddColumn(tab.Column('Mod'))
    TruthCat.AddColumn(tab.Column('type'), name='OBJTYPE')
    TruthCat.AddColumn(tab.Column('z'))
    TruthCat.AddColumn(args.seed, name='SEED', fmt='J')
    TruthCat.AddColumn(args.zeropoint, name='ZEROPOINT', fmt='E')
    #TruthCat.AddColumn(args.tile, name='TILENAME', fmt='12A')
    TruthCat.AddColumn(args.ra, name='RA', fmt='E')
    TruthCat.AddColumn(args.dec, name='DEC', fmt='E')

    if args.detbands==None:
        TruthCat.AddColumn(tab.Column(args.mag), name='MAG')
    else:
        TruthCat.AddColumn( Function(function=Flux2Mag, args=[sampled.magnitude,args]), name='MAG', fmt='E' )



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


