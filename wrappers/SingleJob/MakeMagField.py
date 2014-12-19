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

    #k2 = kx*kx + ky*ky + kz*kz
    #k2 = kx*kx + ky*ky + kzed*kzed
    
    phase = np.angle(kappa_fft)
    #amp2 = np.exp( -k2/(kscale*kscale) )
    #kappa_fft = np.sqrt(amp2) * np.exp(1j*phase)
    amp = np.sqrt( np.exp( -(kx*kx + ky*ky + kz*kz) / (kscale*kscale) ) )
    kappa_fft = amp * np.exp(1j*phase)
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
    return amp_scale*kappa/sig


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

    #zfile = '/direct/astro+u/esuchyta/git_repos/BalrogSetupBNL/input_cats/CMC_sersic.fits'
    #redshifts = pyfits.open(zfile)[1].data['z']
    #zmin = np.amin(redshifts)
    #zmax = np.amax(redshifts)
    zmin = 0.0
    zmax = 6.0

    r = (1+zmax) * cosmo.angular_diameter_distance(zmax).value
    print r * 15.1 * np.pi/180.0
    print r * 1.5 * np.pi/180.0

    '''
    plt.figure(1)
    reds = np.arange(zmin, zmax, step=0.05)
    s = 1.5 * np.pi / 180.0 * cosmo.angular_diameter_distance(reds).value
    plt.plot(reds, s)

    plt.figure(2)
    plt.hist(redshifts, bins=20)
    plt.show()
    '''

    dimx = 1024
    dimy = 128
    dimz = 512
    k_scale = 0.01
    amp_scale = 0.01
  
    zlim = np.array([zmin,zmax])
    zlim = (1+zlim) * cosmo.angular_diameter_distance(zlim).value
    deltaz = (zlim[1] - zlim[0]) / (dimz-1)
    index = np.arange(dimz)

    dims = (dimx, dimy, dimz)
    delta = np.array([2.1, 2.0, deltaz]) # in Mpc, Mpc, log
    kappa = GetKappaField(dims, delta, kscale=k_scale, amp_scale=amp_scale)
    hdu = pyfits.PrimaryHDU(np.real(kappa))
    hdu.header['dx'] = delta[0]
    hdu.header['dy'] = delta[1]
    hdu.header['dz'] = delta[2]
    hdu.header['dimx'] = dimx
    hdu.header['dimy'] = dimy
    hdu.header['dimz'] = dimz
    hdulist = pyfits.HDUList([hdu])

    file = 'magfield.fits'
    if os.path.exists(file):
        os.remove(file)
    hdulist.writeto(file)
    

if __name__ == "__main__":

    MakeField()
    
