#!/usr/bin/env python

import os
import sys
import numpy as np
import pywcs
import esutil
import scipy.special
from model_class import *

usemorph = False


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
    parser.add_argument( "-detb", "--detbands", help="detection bands", type=str, default=None)
    parser.add_argument( "-detz", "--detzeropoints", help="zeropoints for detection bands", type=str, default=None)
    parser.add_argument( "-detws", "--detweights", help="weights for detection bands", type=str, default=None)
    parser.add_argument( "-detfs", "--detfiles", help="files for detection bands", type=str, default=None)

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

    if args.band == 'det':
        if args.detbands==None:
            raise Exception("when using --band = 'det', you must also give --detbands")
        if args.detzeropoints==None:
            raise Exception("when using --band = 'det', you must also give --detzeropoints")

    if args.detbands!=None:
        args.detbands = args.detbands.split(',')
        args.detzeropoints = args.detzeropoints.split(',')
        args.detweights = args.detweights.split(',')
        args.detfiles = args.detfiles.split(',')
        for i in range(len(args.detzeropoints)):
            args.detzeropoints[i] = float(args.detzeropoints[i])
            args.detweights[i] = float(args.detweights[i])
        args.detweigths = np.array(args.detweights)
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
    wcoords = wcs.wcs_sky2pix(wcoords, 1)
    return wcoords

def GetXCoords(args):
    wcoords = GetImageCoords(args)
    return wcoords[:,0]

def GetYCoords(args):
    wcoords = GetImageCoords(args)
    return wcoords[:,1]

def GetBias(n):
    #n = float(n)
    bias = np.sqrt(2) * scipy.special.gamma((n+1)/2) / scipy.special.gamma(n/2)
    return bias

def MultibandMag(mz, args, x, y):
    flux = np.zeros(args.ngal)
    #w = np.sqrt(args.detweights)
    #w = args.detweights

    nums = np.zeros(args.ngal)
    for i in range(len(mz)):
        mag = mz[i]

        #f = np.power(10.0, (args.zeropoint - mag) / 2.5) * np.power(10.0, (args.zeropoint - args.detzeropoints[i]) / 2.5)
        f = np.power(10.0, (args.detzeropoints[i] - mag) / 2.5)

        ws = pyfits.open(args.detfiles[i])[args.weightext].data
        #w = np.random.choice(ws.flatten())
        w = ws[np.int32(y),np.int32(x)]


        #flux += f
        #flux += f * args.detweights[i]
        #flux += f * w[i]
        #flux += f*f * w[i]

        cut = (w > 0)
        nums[cut] = nums[cut] + 1
        flux[cut] =  flux[cut] + np.power(f[cut],2.0) * w[cut]

    #flux = flux / len(mz)
    #flux = flux / np.sum(args.detweights)
    #flux = flux / np.sum(w)
    #flux = (np.sqrt(flux) - bias) / np.sqrt(num - bias*bias)

    bias = GetBias(nums)
    cut = (nums > 0)
    flux[cut] = (np.sqrt(flux[cut]) - bias[cut]) / np.sqrt(nums[cut] - bias[cut]*bias[cut])
    #mag = Flux2Mag(flux, args)
    #return mag
    return flux

def Flux2Mag(flux, args):
    mag = np.array( [99.0]*args.ngal )
    cut = (flux > 0)
    mag[cut] = args.zeropoint - 2.5 * np.log10(flux[cut])
    return mag


def SimulationRules(args, rules, sampled, TruthCat):
    cat = args.catalog
    ext = args.ext
    tab = Table(file=args.catalog, ext=args.ext)

    rules.g1 = 0
    rules.g2 = 0
    rules.magnification = 1

    if args.ngal > 0:
        rules.x = Function(function=GetXCoords, args=[args])
        rules.y = Function(function=GetYCoords, args=[args])
    else:
        rules.x = 0
        rules.y = 0
    
    rules.halflightradius = tab.Column(args.reff)
    rules.sersicindex = tab.Column(args.sersicindex)
    if usemorph:
        rules.beta = tab.Column(args.beta)
        rules.axisratio = tab.Column(args.axisratio)
    else:
        rules.beta = 0
        rules.axisratio = 1

    if args.band=='det':
        bz = []
        for i in range(len(args.detbands)):
            m = ByBand(args.detbands[i])
            #z = args.detzeropoints[i]
            #bz.append( [tab.Column(m),z] )
            bz.append( tab.Column(m) )
        rules.magnitude = Function(function=MultibandMag, args=[bz,args,sampled.x,sampled.y])
    else:
        rules.magnitude = tab.Column(args.mag)


    if usemorph:
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

    if args.band!='det':
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


