Updated README coming. There have been changes to more or less everything. I'll try to document once it's all finalized.

## Generating the Balrog job

Use [BuildJob.py](https://github.com/suchyta1/BalrogMPI/blob/nompi/BuildJob.py) to build the Balrog jobs to submit to the queue scheduler.
In priciple, these jobs have a practically infinite number of adjustable parameters, but I've attempted to set the defaults to be what you probably want,
particularly if you're running at BNL or NERSC, which is probably the case. (Only crazy people like me venture to set up stuff like this on new systems.)
