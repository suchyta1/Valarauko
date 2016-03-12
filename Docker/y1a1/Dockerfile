FROM debian:7.9
MAINTAINER Eric Suchyta <eric.d.suchyta@gmail.com>

ENV valarauko /Valarauko-job
RUN  mkdir /software && \
	apt-get update && apt-get install -y wget libaio-dev python-dev libfftw3-dev libboost-all-dev libblas-dev scons git make libatlas-base-dev python-numpy pkg-config libfreetype6-dev libpng12-dev && \
	mv /bin/sh /bin/sh.orig && ln -s /bin/bash /bin/sh && \
	cd /software && wget https://bootstrap.pypa.io/get-pip.py && python get-pip.py && rm get-pip.py && \
	#pip install --no-deps --upgrade numpy && \
	pip install --no-deps astropy six pyparsing cycler python-dateutil matplotlib fitsio healpy && \
	git clone https://github.com/esheldon/esutil.git && cd esutil && python setup.py install && cd /software && rm -r esutil && \
	python -c "import healpy" && \

	cd /software && wget http://www.cosmo.bnl.gov/www/esheldon/code/misc/des-oracle-linux-x86-64-v2.tar.gz && \
	tar xvzf des-oracle-linux-x86-64-v2.tar.gz && rm des-oracle-linux-x86-64-v2.tar.gz && cd des-oracle-linux-x86-64-v2 && ./do-install /software/des-oracle-linux-x86-64-v2/install && \
	rm -r instantclient_11_2/ && rm -r cx_Oracle-5.1.1-ess/ && rm cx_Oracle-5.1.1-ess.tar.gz && \
	cd /software && git clone https://github.com/esheldon/desdb.git && cd desdb/ && python setup.py install && cd /software && rm -r desdb/ && \
	cd /software && git clone https://github.com/suchyta1/suchyta_utils.git && cd suchyta_utils && python setup.py install && cd /software && rm -r suchyta_utils && \
	git clone https://github.com/emhuff/Balrog.git && cd Balrog/ && rm -r default_example/ && rm -r astro_config/ && rm cosmos* && yes | rm -r .git/ && \
	cd /software && git clone https://github.com/suchyta1/Valarauko.git && \

	# This version of TMV was built by me and then I just used the binaries to save build time when testing.
	wget https://googledrive.com/host/0B4AAwvZlUdfeWFNiN1RLZUpVOU0 -O tmv-0.72-debian.tar.gz && tar xvzf tmv-0.72-debian.tar.gz && rm tmv-0.72-debian.tar.gz && \
	git clone https://github.com/GalSim-developers/GalSim.git && cd GalSim && scons TMV_DIR=/software/tmv-0.72-debian && scons install && cd /software && rm -r GalSim/ && \

	#mkdir tmv && wget https://googledrive.com/host/0B6hIz9tCW5iZdEcybFNjRHFmOEE/tmv0.72.tar.gz && tar xvzf tmv0.72.tar.gz && rm tmv0.72.tar.gz && \
	#cd tmv0.72 && scons install PREFIX=/software/tmv && cd /software && rm -r tmv0.72 && \
	#git clone https://github.com/GalSim-developers/GalSim.git && cd GalSim && scons TMV_DIR=/software/tmv && scons install  && cd /software && rm -r GalSim/ && \

	wget --no-check-certificate 'https://googledrive.com/host/0B4AAwvZlUdfeT2V0ZTVhTlFDc1E' -O swarp-2.36.2.tar.gz && tar xvzf swarp-2.36.2.tar.gz && rm swarp-2.36.2.tar.gz && \
	cd swarp-2.36.2/ && ./configure && make && make install && cd /software && rm -r swarp-2.36.2/ && \
	wget --no-check-certificate 'https://googledrive.com/host/0B4AAwvZlUdfeeURxS3RsZWw5VDg' -O sextractor-2.18.10.tar.gz && tar xvzf sextractor-2.18.10.tar.gz && rm sextractor-2.18.10.tar.gz && \
	cd sextractor-2.18.10/ && ./configure --with-atlas-incdir=/usr/include/atlas && make && make install && cd /software && rm -r sextractor-2.18.10/ && \
	wget --no-check-certificate 'https://googledrive.com/host/0B4AAwvZlUdfeOV9QQmxGNmlrcUE' -O cfitsio-3.360.tar.gz && tar xvzf cfitsio-3.360.tar.gz && rm cfitsio-3.360.tar.gz && \
	cd cfitsio && ./configure && make && make funpack && cp funpack /usr/bin && cd /software && rm -r cfitsio && \

	python -m pip uninstall -y pip setuptools wheel && rm -r /root/.cache/pip/ && \
	apt-get --purge autoremove -y libblas-dev scons git make libatlas-base-dev pkg-config libpng12-dev && apt-get -y clean && \
	apt-get remove --purge -y libicu-dev g++ libboost1.49-dev python2.7-dev libicu48 gcc-4.7 g++-4.4 libopenmpi-dev cpp-4.7 libc6-dev cpp-4.4 libopenmpi1.3 binutils libmagic1 linux-libc-dev manpages-dev libfftw3-dev libboost-wave1.49.0 libboost-math1.49.0 libboost-mpi-python1.49.0 libboost-test1.49.0 libboost-serialization1.49.0 libboost-program-options1.49.0 && \


	mkdir $valarauko && mkdir $valarauko/jobroot && mkdir $valarauko/outroot && mkdir $valarauko/slrroot && mkdir $valarauko/catroot && mkdir $valarauko/posroot && \
	mkdir /home/user && mkdir /home/user/site && \

	#echo 'if [ -f ~/.bashrc ]; then source ~/.bashrc; fi' >> ~/.bash_profile && \
	#echo 'export PYTHONPATH' >> /software/des-oracle-linux-x86-64-v2/install/setup.sh && \
	#echo 'source /software/des-oracle-linux-x86-64-v2/install/setup.sh' > ~/.bashrc && \
	#echo 'export PYTHONPATH=/software/Balrog:${PYTHONPATH}' >> ~/.bashrc && \
	#echo 'export DESREMOTE=https://desar2.cosmology.illinois.edu:/DESFiles/desardata' >> ~/.bashrc && \
	#echo 'export DESPROJ=OPS' >> ~/.bashrc && \
	#echo 'export HOME=/home/user/site' >> ~/.bashrc && \
	#cp /root/.bashrc /home/user/.bashrc && chmod 755 /home/user/.bashrc && \
	#cp /root/.bash_profile /home/user/.bash_profile && chmod 755 /home/user/.bash_profile 

	echo 'export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/software/des-oracle-linux-x86-64-v2/install/instantclient_11_2' > /home/user/.bashrc && \
	echo 'export LD_LIBRARY_PATH=/software/des-oracle-linux-x86-64-v2/install/instantclient_11_2' >> /home/user/.bashrc && \
	echo 'export C_INCLUDE_PATH=/software/des-oracle-linux-x86-64-v2/install/instantclient_11_2' >> /home/user/.bashrc && \
	echo 'export PYTHONPATH=/software/Balrog:/software/des-oracle-linux-x86-64-v2/install/cx_Oracle-5.1.1-ess' >> /home/user/.bashrc && \
	echo 'export ORACLE_HOME=/software/des-oracle-linux-x86-64-v2/install/instantclient_11_2' >> /home/user/.bashrc && \
	echo 'export DESREMOTE=https://desar2.cosmology.illinois.edu:/DESFiles/desardata' >> /home/user/.bashrc && \
	echo 'export DESPROJ=OPS' >> /home/user/.bashrc && \
	echo 'export HOME=/home/user/site' >> /home/user/.bashrc && \
	chmod 755 /home/user/.bashrc

