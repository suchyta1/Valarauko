#!/usr/bin/env python

import esutil
import desdb
import numpy as np

if __name__ == "__main__":
   
    cur = desdb.connect()

    q = """                         
    SELECT 
        column_name,
        CAST(data_type as VARCHAR2(15)) as type, 
        CAST(data_length as VARCHAR(6)) as length, 
        CAST(data_precision as VARCHAR(9)) as precision, 
        CAST(data_scale as VARCHAR(5)) as scale
    FROM
        table(fgetmetadata)
    WHERE
        table_name  = 'SVA1_COADD_OBJECTS'
    ORDER BY
        column_id
    """
    arr = cur.quick(q, array=True)

    print arr['column_name']
    print len(arr)

    #np.savetxt('y1a1_coadd_objects-columns.txt', arr)
    #esutil.io.write('y1a1_coadd_objects-columns.fits', arr)

    esutil.io.write('sva1_coadd_objects-columns.fits', arr)
