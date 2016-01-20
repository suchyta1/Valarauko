#!/usr/bin/env python

import desdb
import numpy as np
import esutil

if __name__ == "__main__":
    cur = desdb.connect()
   
    bands = ['g', 'r', 'i', 'z', 'Y']
    release = 'SVA1_COADD_RXJ'
    tiles = {}

    for band in bands:
        y = cur.quick("SELECT c.run, c.tilename, c.ra, c.dec from coadd c, runtag rt where rt.run=c.run and rt.tag='%s' and c.band='%s'ORDER BY c.dec DESC, c.ra ASC " %(release,band), array=True)
        tiles[band] = y['tilename']
        

    stiles = tiles[bands[0]]
    for band in bands[1:]:
        sts = tiles[band]
        skeep = np.in1d(stiles, sts)
        stiles = stiles[skeep]

    data = np.zeros(len(stiles), dtype=[('tilename', '|S12')])
    data['tilename'] = stiles
    esutil.io.write('%s-grizY.fits'%(release.lower()), data, clobber=True)
