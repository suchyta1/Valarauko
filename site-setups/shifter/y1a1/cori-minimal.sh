source /project/projectdirs/cmb/modules/hpcports_NERSC.sh
hpcports gnu 
module load hpcp
module load python-hpcp
module load numpy-hpcp
module load astropy-hpcp
export PYTHONPATH=/global/cscratch1/sd/zuntz/stack/usr/lib/python2.7/site-packages:${PYTHONPATH}
