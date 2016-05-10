function get_hpcp
{
	source /project/projectdirs/cmb/modules/hpcports_NERSC.sh
	hpcports gnu
}

function get_suchyta
{
	module use /global/cscratch1/sd/esuchyta/cori-modules
	export SUCHYTA_SOFTWARE=/global/cscratch1/sd/esuchyta/cori-software
}

function screen_setup
{
	get_suchyta
	module load screen/local
}


function balrog_y1a1_setup
{
	get_hpcp
	module load hpcp
	module load python-hpcp
	module load numpy-hpcp
	module load mpi4py-hpcp
	module load astropy-hpcp
	module load boost-hpcp

	export DESREMOTE=https://desar2.cosmology.illinois.edu:/DESFiles/desardata
	export DESPROJ=OPS 
	#export DESDATA=${SCRATCH}/DES/desdata
	#export BALROG_DESDATA=${SCRATCH}/DES/balrog_desdata

	export STACK_BASE_DIR=/global/cscratch1/sd/zuntz/stack/
	export PYTHONPATH=$PYTHONPATH:${STACK_BASE_DIR}/usr/lib/python2.7/site-packages
	export PATH=$PATH:${STACK_BASE_DIR}/usr/bin
	export ORACLE_HOME=${STACK_BASE_DIR}/oracle/instantclient_12_1
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME
	export LD_LIBRARY_PATH=$STACK_BASE_DIR/usr/lib:$LD_LIBRARY_PATH

	get_suchyta
	module load desdm-eups/local
	source ${DESDM_EUPS}/desdm_eups_setup.sh
	module load desdm-config/y1a1
	module load kmeans_radec/master
	module load basemap/1.0.7
	module load scikit-learn/0.17
	module load pywcs/1.12
	source ${SUCHYTA_SOFTWARE}/desdb/des-oracle-linux-x86-64-v2/install/setup.sh
	module load desdb/master
	module load suchyta_utils/master
	module load balrog/master
	module load BalrogMPI/master

	#module load healpy-hpcp
	export PYTHONPATH=/global/cscratch1/sd/esuchyta/.local/lib/python2.7/site-packages/:${PYTHONPATH}
	umask u=wrx,g=rx,o=rx
}

function hdf5_setup
{
	module load hdf5-parallel
	module unload h5py
	module load h5py-parallel
	module load pandas
}


function balrog_test
{
	get_hpcp
	
	module load screen/local

	module load galsim-hpcp
	module load sextractor-hpcp
	module load astropy-hpcp
}

balrog_y1a1_setup
