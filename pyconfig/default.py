#!/usr/bin/env python

import os
import sys
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

    parser.add_argument( "-posstart", "--posstart", help="Index to start in position catalog", type=int, default=1)
    parser.add_argument( "-poscat", "--poscat", help="Position catalog", type=str, required=True)
    parser.add_argument( "-posext", "--posext", help="Position extension", type=int, default=1)
    parser.add_argument( "-xkey", "--xkey", help='x col', type=str, default='x')
    parser.add_argument( "-ykey", "--ykey", help='y col', type=str, default='y')
    parser.add_argument( "-rakey", "--rakey", help='ra col', type=str, default='ra')
    parser.add_argument( "-deckey", "--deckey", help='dec col', type=str, default='dec')

    parser.add_argument( "-magn", "--magnification", help='Constant magnification (i.e. 1+*magn, so mang=2k)', type=float, default=0.0)



def CustomParseArgs(args):
    thisdir = os.path.dirname( os.path.realpath(__file__) )
    if args.catalog==None:
        #args.catalog = '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/input_cats/CMC_sersic_alltypes.fits'
        #args.catalog = '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/input_cats/CMC_allband_upsample_SG.fits'
        args.catalog = '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/input_cats/CMC_allband_upsample.fits'

    if args.band=='Y':
        args.mag = 'Mapp_HSC_y'
    else:
        args.mag = 'Mapp_%s_subaru' %(args.band)

    coords = np.array( pyfits.open(args.poscat)[args.posext].data[args.posstart:(args.posstart+args.ngal)] )
    args.x = coords[args.xkey]
    args.y = coords[args.ykey]
    args.ra = coords[args.rakey]
    args.dec = coords[args.deckey]



def SimulationRules(args, rules, sampled, TruthCat):
    cat = args.catalog
    ext = args.ext
    tab = Table(file=args.catalog, ext=args.ext)

    rules.x = args.x
    rules.y = args.y
    rules.g1 = 0
    rules.g2 = 0
    redshift = tab.Column('z')
    rules.magnification = 1.0 + args.magnification
    
    rules.halflightradius = tab.Column(args.reff)
    rules.magnitude = tab.Column(args.mag)
    rules.beta = tab.Column(args.beta)
    rules.axisratio = tab.Column(args.axisratio)
    rules.sersicindex = tab.Column(args.sersicindex)

    TruthCat.AddColumn(tab.Column('Id'))
    TruthCat.AddColumn(tab.Column('Mod'))
    TruthCat.AddColumn(tab.Column('type'), name='OBJTYPE')
    TruthCat.AddColumn(redshift)
    TruthCat.AddColumn(tab.Column(args.mag), name='MAG')
    TruthCat.AddColumn(args.seed, name='SEED', fmt='J')
    TruthCat.AddColumn(args.zeropoint, name='ZEROPOINT', fmt='E')
    TruthCat.AddColumn(args.tile, name='TILENAME', fmt='12A')
    TruthCat.AddColumn(args.ra, name='RA', fmt='E')
    TruthCat.AddColumn(args.dec, name='DEC', fmt='E')



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


