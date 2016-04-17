#!/usr/bin/env python

import desdb
import numpy as np
import esutil
import fitsio
import os
import numpy.lib.recfunctions as rec

if __name__ == "__main__":

    cur = desdb.connect()
    bands = ['g', 'r', 'i', 'z', 'Y']
    notcommon = {}
    for band in bands:
        y = cur.quick("SELECT c.run, c.tilename, c.ra, c.dec from coadd c, runtag rt where rt.run=c.run and rt.tag='Y1A1_COADD_SPT' and c.band='%s' ORDER BY c.dec DESC, c.ra ASC" %(band), array=True)
        notcommon[band] = y

    tiles = notcommon[bands[0]]['tilename']
    ras = notcommon[bands[0]]['ra']
    decs = notcommon[bands[0]]['dec']

    for band in bands[1:]:
        ts = notcommon[band]['tilename']
        keep = np.in1d(tiles, ts)
        tiles = tiles[keep]
        ras = ras[keep]
        decs = decs[keep]
        
    data = np.zeros(len(tiles), dtype=[('tilename', '|S12'), ('ra',np.float32), ('dec',np.float32)])
    data['tilename'] = tiles
    data['ra'] = ras
    data['dec'] = decs


    tiledir = '../tiles'
    otiles = fitsio.read(os.path.join(tiledir,'spt-sva1+y1a1-overlap-grizY.fits'), ext=1, columns=['tilename'])
    ntiles = fitsio.read(os.path.join(tiledir,'spt-y1a1-only-g70-grizY.fits'), ext=1, columns=['tilename'])
    cut = (~np.in1d(data['tilename'],otiles['tilename'])) & (~np.in1d(data['tilename'],ntiles['tilename']))
    data = data[cut]

    '''
    outfile = os.path.join(tiledir,'y1a1-spt-west-grizY.fits')
    if os.path.exists(outfile):
        os.remove(outfile)
    f = fitsio.FITS(outfile, 'rw')
    f.write(data)
    '''
    
    r = np.copy(data['ra'])
    neg = (r > 180)
    r[neg] = r[neg] - 360
    data = rec.append_fields(data, 'neg', r)
    data = np.sort(data, order='neg')
    data = rec.drop_fields(data, 'neg')


    d1 = data[-1000:]
    outfile = os.path.join(tiledir,'y1a1-sptw1-grizY.fits')
    if os.path.exists(outfile):
        os.remove(outfile)
    f = fitsio.FITS(outfile, 'rw')
    f.write(d1)

    d2 = data[:-1000]
    outfile = os.path.join(tiledir,'y1a1-sptw2-grizY.fits')
    if os.path.exists(outfile):
        os.remove(outfile)
    f = fitsio.FITS(outfile, 'rw')
    f.write(d2)
