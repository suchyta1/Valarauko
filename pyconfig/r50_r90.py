#!/usr/bin/env python

import os
import numpy as np
from model_class import *


def CustomArgs(parser):
    parser.add_argument( "-cs", "--catalog", help="Catalog used to sample simulated galaxy parameter distriubtions from", type=str, default=None)
    parser.add_argument( "-ext", "--ext", help="Index of the data extension for sampling catalog", type=int, default=1)

    parser.add_argument( "-reff", "--reff", help="Column name when drawing half light radius from catalog", type=str, default="halflightradius")
    parser.add_argument( "-nsersic", "--sersicindex", help="Column name when drawing sersic index catalog", type=str, default="sersicindex")
    parser.add_argument( "-ax", "--axisratio", help="Axis ratio column", type=str, default="axisratio")
    parser.add_argument( "-beta", "--beta", help="Beta column", type=str, default="beta")

    parser.add_argument( "-b", "--band", help="Which filter band to choose from COSMOS catalog. Only relevant if --mag is not given and using COSMOS catlalog.", type=str, default='i', choices=['g','r','i','z','Y'])

    parser.add_argument( "-tl", "--tile", help="Tilename", type=str, required=True)
    parser.add_argument( "-kp", "--kappa", help="kappa for magnification", type=float, default=0.0)


def CustomParseArgs(args):
    thisdir = os.path.dirname( os.path.realpath(__file__) )
    if args.catalog==None:
        args.catalog = '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/input_cats/CMC_sersic.fits'

    if args.band=='Y':
        args.mag = 'Mapp_HSC_y'
    else:
        args.mag = 'Mapp_%s_subaru' %(args.band)
    

def SimulationRules(args, rules, sampled, TruthCat):
    cat = args.catalog
    ext = args.ext

    rules.x = Function(function=rand, args=[args.xmin, args.xmax, args.ngal])
    rules.y = Function(function=rand, args=[args.ymin, args.ymax, args.ngal])
    rules.g1 = 0
    rules.g2 = 0
    rules.magnification = 1 + args.kappa
    
    tab = Table(file=args.catalog, ext=args.ext)
    rules.halflightradius = tab.Column(args.reff)
    rules.magnitude = tab.Column(args.mag)
    rules.sersicindex = tab.Column(args.sersicindex)
    rules.beta = tab.Column(args.beta)
    rules.axisratio = tab.Column(args.axisratio)

    TruthCat.AddColumn(tab.Column('Id'))
    TruthCat.AddColumn(tab.Column('type'), name='OBJTYPE')
    TruthCat.AddColumn(tab.Column('z'))
    TruthCat.AddColumn(tab.Column(args.mag), name='MAG')
    TruthCat.AddColumn(args.seed, name='SEED', fmt='J')
    TruthCat.AddColumn(args.zeropoint, name='ZEROPOINT', fmt='E')
    TruthCat.AddColumn(args.tile, name='TILENAME', fmt='12A')

    if args.ngal > 0:
        ra = Function(function=GetRA, args=[sampled.x,sampled.y,args.image,args.imageext])
        dec = Function(function=GetDEC, args=[sampled.x,sampled.y,args.image,args.imageext])
    else:
        ra = 0
        dec = 0
    TruthCat.AddColumn(ra, name='RA', fmt='E')
    TruthCat.AddColumn(dec, name='DEC', fmt='E')


def GetRA(x, y, file, ext):
    return GetWCS(x,y,file,ext)[:,0]

def GetDEC(x, y, file, ext):
    return GetWCS(x,y,file,ext)[:,1]


def GetWCS(x, y, file, ext):
    import pyfits
    import pywcs
    hdu = pyfits.open(file)[ext]
    header = hdu.header
    wcs = pywcs.WCS(header)
    pcoords = np.dstack((x,y))[0]
    wcoords = wcs.wcs_pix2sky(pcoords, 1)
    return wcoords


def rand(minimum, maximum, ngal):
    return np.random.uniform( minimum, maximum, ngal )


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


