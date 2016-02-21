## Generating simulation positions

In the new setup, positions are NOT generated on the fly by the job that actually runs Balrog.
One creates a set of positions for a set of tiles before doing that run set,
and then the scripts which run Balrog read these positions.
This way, one can generate random positions which are truly uniform over the sphere, with nothing enforced on tile scale.
Though, I have built in an option to still run with equal number per tile settings to be "backward compatible".
(This uses ```--pertile```, but I don't recommend using this. Objects are placed into the "unique areas" defined by DESDM,
whih are not exactly equal area, so one ends up with slightly non-uniform sampling.)

There is a file called [```BuildPos.py```](https://github.com/suchyta1/BalrogMPI/blob/nompi/BuildPos.py), which generates the positions.
Run ```BuildPos.py --help``` for the command line arguments. They should be relatively clear.
If you use the same ```--seed```, ```--density/--pertile```, (and ```--iterateby``` if using ```--density```),
with the same file given in ```--tiles```, you'll get the same positions. 
If you append to the ```--tiles``` file and run again, you'll ultimate generate ```balrog_index``` values which are consistent for the common tiles.

I haven't supplied a script to generate jobs for [```BuildPos.py```](https://github.com/suchyta1/BalrogMPI/blob/nompi/BuildPos.py), 
because you don't need to do this very often, and it's not very complex. You'll want an ```mpirun``` (or ```srun```, or whatever) something like below.
I use ```mpi``` because for ```--sampling sphere```, generating points over the whole sphere can be a lot of points / use a lot of memory, 
so one iterates, and uses multiple machines/cores.
(I could add code to only generate within the RA/DEC boundaries the tiles actually occupy, to make this more efficient, but I haven't done that yet.)

### Example

```
mpirun -np 150 -hostfile %hostfile% ./BuildPos.py --sampling sphere --seed 100 --tiles /some/directory/spt-y1a1-only-g70-grizY.fits --outdir /somewhere/spt-y1a1-only-g70-grizY-pos-sphere/
```

## Generating the Balrog job

Use [```BuildJob.py```](https://github.com/suchyta1/BalrogMPI/blob/nompi/BuildJob.py) to build the Balrog jobs to submit to the queue scheduler.
In principle, these jobs have a practically infinite number of adjustable parameters, but for many of them I've set the defaults to be what you probably want.
You shouldn't need to ever look at a lot of the settings, so they're indeed hidden from you to start.
There are also a lot of dependencies. (I have everything needed installed on the Astro cluster at BNL and Edison at NERSC.)
One can supply a bash script to be sourced to set up all the software at the computing site.
This is done with the ```--source``` command line argument.
(See [```site-setups/Edison/y1-source```](https://github.com/suchyta1/BalrogMPI/blob/nompi/site-setups/Edison/y1-config.py) as an example I use on Edison.)
If ```--source``` isn't given, you'll need to have everything set up by default in your rc file.
To be explicitly clear, the file given will be sourced both when  you're running [```BuildJob.py```](https://github.com/suchyta1/BalrogMPI/blob/nompi/BuildJob.py) itself,
and in your output job submission file. (The former is so it's easier to build your jobs from the command line, without requiring you to manually set things up.)

[```BuildJob.py```](https://github.com/suchyta1/BalrogMPI/blob/nompi/BuildJob.py) takes up to 3 command line arguments.
```--source``` was mentioned above.
You'll always need to give an argument to ```--config```. This is a python file 
(see [```site-setups/Edison/y1-config.py```](https://github.com/suchyta1/BalrogMPI/blob/nompi/site-setups/Edison/y1-config.py) as an example I use on Edison), 
which sets up configuration dictionaries (except ```tiles```, which is an array) for all the run parameters. You edit a function called ```CustomConfig```.
Technically speaking all of the entries in the dictionaries have default values, and you're changing these,
but there's essentially no set of defaults which possibly actually makes sense.
(See [```source-code/RunConfigurations.py```](https://github.com/suchyta1/BalrogMPI/blob/nompi/source-code/RunConfigurations.py) for most of the defaults,
but some are intentionally more so hidden in [```/source-code/GenerateJob.py```](https://github.com/suchyta1/BalrogMPI/blob/nompi/source-code/GenerateJob.py).)

The third command line argument is ```--scheduler```, which speicifies the work queue you're submitting to. 
Currently only ```['slurm','wq']``` are accepted. 
If you're at BNL or NERSC you can forget this even exists, and the script will auto-detect what to do.
A working example on Edison would look something like:

### Example

```
./BuildJob.py --config site-setups/Edison/y1-config.py --source site-setups/Edison/y1-source.sh
```
