#!/usr/bin/env python

import desdb
import numpy as np
import esutil

if __name__ == "__main__":
    cur = desdb.connect()
   
    bands = ['g', 'r', 'i', 'z', 'Y']
    common = {}
    s82 = {}
    for band in bands:
        s = cur.quick("SELECT c.run, c.tilename, c.ra, c.dec from coadd c, runtag rt where rt.run=c.run and rt.tag='SVA1_COADD_SPTE' and c.band='%s' ORDER BY c.dec DESC, c.ra ASC" %(band), array=True)
        y = cur.quick("SELECT c.run, c.tilename, c.ra, c.dec from coadd c, runtag rt where rt.run=c.run and rt.tag='Y1A1_COADD_SPT' and c.band='%s' ORDER BY c.dec DESC, c.ra ASC" %(band), array=True)
        both = np.in1d(s['tilename'], y['tilename'])
        common[band] = s['tilename'][both]

        '''
        if band==bands[0]:
            print len(s), s, '\n\n\n'
            print len(y), y, '\n\n\n'
        '''

        y = cur.quick("SELECT c.run, c.tilename, c.ra, c.dec from coadd c, runtag rt where rt.run=c.run and rt.tag='Y1A1_COADD_STRIPE82' and c.band='%s'ORDER BY c.dec DESC, c.ra ASC " %(band), array=True)
        s82[band] = y['tilename']
        
        '''
        if band==bands[0]:
            print len(y), y
        '''

    tiles = common[bands[0]]
    stiles = s82[bands[0]]
    for band in bands[1:]:
        ts = common[band]
        keep = np.in1d(tiles, ts)
        tiles = tiles[keep]
        
        sts = s82[band]
        skeep = np.in1d(stiles, sts)
        stiles = stiles[skeep]

    data = np.zeros(len(tiles), dtype=[('tilename', '|S12')])
    data['tilename'] = tiles
    esutil.io.write('spt-sva1+y1a1-overlap-grizY.fits', data, clobber=True)

    data = np.zeros(len(stiles), dtype=[('tilename', '|S12')])
    data['tilename'] = stiles
    esutil.io.write('y1a1-stripe82-grizY.fits', data, clobber=True)
