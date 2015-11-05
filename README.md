Some quick notes about some of the files:
* default settings are in RunConfigurations, modify either BNLCustomConfig.py or NERSCCustomConfig.py to change them
* GenerateJob.py parses your setup, and writes the job file for your number of nodes and processes per node. It takes either BNL or NERSC as an argument, and writes the wq or pbs job file. BNL is the default
* AutoJob.py generates your file and submits it to the queue
* AllMpi.py has the MPI server communication
* RunBalrog.py has the functions for running Balrog on the worker CPUs.
* mpifunctions.py has some functions I wrote to make life easier working with MPI4py, but which is mostly irrelevant in the lastest version
