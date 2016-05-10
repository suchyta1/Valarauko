source /scratch1/scratchdirs/esuchyta/.local/setup/local-setup.sh
local_setup --libs
export Y1A1_DIR="/scratch1/scratchdirs/esuchyta/software/balrog_config/y1a1"
export PATH="${Y1A1_DIR}/bin:${PATH}"
umask u=wrx,g=rx,o=rx
