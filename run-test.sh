#!/bin/bash -l

source ~/.bashrc
export PATH=/software/Balrog:/software/eups/packages/Linux64/cfitsio/3.370+0/bin:/software/eups/packages/Linux64/sextractor/2.18.10+16/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/software/des-oracle-linux-x86-64-v2/install/instantclient_11_2:/software/eups/1.2.30/bin:/software/eups/packages/Linux64/fftw/3.3.2+5/bin:/software/eups/packages/Linux64/swarp/2.36.2+3/bin:{$PATH}
echo $PATH
/software/Valarauko/TestMulti.py
