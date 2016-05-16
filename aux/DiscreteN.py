#!/usr/bin/env python

import os
import fitsio
import numpy as np

if __name__ == "__main__":
    
    cat = fitsio.read('/astro/u/jelena/Balrog/Catalogs/CMC_originalR_v1.fits', ext=1)
    num = 100
    out = 'CMC_originalR_v1_%i-n.fits'%(num)

    bins = np.linspace(0.3, 6.001, num+1)
    d = np.digitize(cat['sersicindex'], bins=bins)
    cent = (bins[1:]+bins[:-1])/2.0
    n = cent[d-1]
    cat['sersicindex'] = n

    if os.path.exists(out):
        os.remove(out)
    fitsio.write(out, cat)
