#!/bin/bash -l 

#SBATCH --image=docker:esuchyta/balrog-docker:v1
#SBATCH --job-name=multitest
#SBATCH --mail-type=BEGIN,END
#SBATCH --partition=debug
#SBATCH --time=00:01:00
#SBATCH --nodes=1
#SBATCH --output=/scratch1/scratchdirs/esuchyta/BalrogJobs/shifter-tests/multitest-%j.out

#source /scratch1/scratchdirs/esuchyta/software/tmp-Balrog/BalrogMPI/site-setups/Edison/y1-source.sh
#export PYTHONPATH=/scratch1/scratchdirs/esuchyta/.local/software:${PYTHONPATH}
#if ! [ -d /scratch1/scratchdirs/esuchyta/BalrogScratch/y1a1_etest-0:1 ]; then mkdir /scratch1/scratchdirs/esuchyta/BalrogScratch/y1a1_etest-0:1; fi;
#lfs setstripe /scratch1/scratchdirs/esuchyta/BalrogScratch/y1a1_etest-0:1 --count 2

srun -N 1 -n 1 shifter --volume=/scratch1/scratchdirs/esuchyta/BalrogJobs/shifter-tests/:/testdir/ /software/Valarauko/TestMulti.py

#wait
