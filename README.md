
Installation requirements:

* I slightly hacked desdb to get an extra function that wasn't callable. Add this line to the try block in desdb/__init__.py:
    from .desdb import get_tabledef
* Add the path where this reposiory lives on disk to your enviorment variables as BALROG_MPI


Some quick notes about some of the files:
* default settings are in RunConfigurations, modify CustomConfig.py to change the run
* GenerateJob.py parses your setup, and writes the job file for your number of nodes and processes per node. It takes either BNL or NERSC as an argument, and writes the wq or pbs job file. BNL is the default
* AutoJob.py generates your file and submits it to the queue
* AllMpi.py has the MPI server communication
* RunBalrog.py has the functions for running Balrog on the worker CPUs.
* mpifunctions.py has some functions I wrote to make life easier working with MPI4py, but which is mostly irrelevant in the lastest version
