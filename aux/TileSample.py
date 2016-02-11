#!/usr/bin/env python

import desdb
import numpy as np
import esutil

if __name__ == "__main__":
    cur = desdb.connect()

    svtiles = esutil.io.read('../tiles/spt-sva1+y1a1-overlap-grizY.fits')
   
    bands = ['g', 'r', 'i', 'z', 'Y']
    notcommon = {}
    s82 = {}
    for band in bands:
        s = cur.quick("SELECT c.run, c.tilename, c.ra, c.dec from coadd c, runtag rt where rt.run=c.run and rt.tag='SVA1_COADD_SPTE' and c.band='%s' ORDER BY c.dec DESC, c.ra ASC" %(band), array=True)
        y = cur.quick("SELECT c.run, c.tilename, c.ra, c.dec from coadd c, runtag rt where rt.run=c.run and rt.tag='Y1A1_COADD_SPT' and c.band='%s' ORDER BY c.dec DESC, c.ra ASC" %(band), array=True)

        #yonly = (-np.in1d(y['tilename'], svtiles['tilename'])) & (y['ra'] > 70) & (y['ra'] < 200)
        yonly = (np.in1d(y['tilename'], s['tilename'])) #& (y['ra'] > 70) & (y['ra'] < 200)
        notcommon[band] = y[yonly]

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

    #esutil.io.write('../tiles/spt-y1a1-only-g70-grizY.fits', data, clobber=True)
    esutil.io.write('../tiles/overlap-tmp.fits', data, clobber=True)
    
