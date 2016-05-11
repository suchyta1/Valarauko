# Valarauko 

> The last etymology, appearing in the invented languages Quendi and Eldar, derives Balrog as the Sindarin translation of the Quenya form Valarauko (Demon of Might). 
> This etymology was published in The Silmarillion. -- [Wikipedia](https://en.wikipedia.org/wiki/Balrog)

Contained here is python code to intended to run [`Balrog`](https://github.com/emhuff/Balrog) en-masse over DES coadds.
The generated `Balrog` jobs run each tile on a separate node, 
parallelizing on the node with python's [`multiprocessing.Pool()`](https://docs.python.org/2/library/multiprocessing.html#module-multiprocessing.pool).
Files are automatically downloaded from the DESDM file server, and results are pushed to the user's space in the `dessci` DB. 
You'll need to setup your [`.netrc`](https://github.com/esheldon/desdb#access-to-servers) file for any of this to work.

The code puts the `Balrog` objects in the `r, i, z` coadd images, then builds the `riz` detection image, with [`swarp`](http://www.astromatic.net/software/swarp).
It also draws into the `g` and `Y` images, then runs [`sextractor`](http://www.astromatic.net/software/sextractor) over each band using the `riz` `Balrog` image for detection.
All of this is done with code that amounts to wrappers around `Balrog` itself. `swarp` and `sextractor` can be configured in the same was as DESDM.
We're also doing `nosim` runs over each band, running `sextractor` prior to inserting the `Balrog` objects.
For both the `nosim` and usual `sim` calls we even run `sextractor` using the dectection image as a measurement image.


## Installation

You can ask me if this is relevant for you; if you're trying to get this working somewhere, probably I know about it and am helping you anyway.
In short, having everything setup is somewhat non-trivial.
The package itself doesn't really require any building -- just cloning the code --
but there are about a gazillion dependencies you need for the code to actually successfully run, most of which are python modules. 
Right now I have everything installed on the [Astro cluster](https://wiki.bnl.gov/astro/index.php/Computing) at BNL, 
and [Edison](http://www.nersc.gov/users/computational-systems/edison/) and  [Cori](http://www.nersc.gov/users/computational-systems/cori/) at NERSC,
(but wget is broken on the compute nodes on Cori, so I'm not using it.)
See [below](https://github.com/suchyta1/Valarauko#generating-the-balrog-job) about integrating the software setup into the `Balrog` runtime.


## Generating simulation positions

In the new setup, positions are NOT generated on the fly by the job that actually runs `Balrog`.
One creates a set of positions for a set of tiles before doing that run set,
and then the scripts which run `Balrog` read these positions.
This way, one can generate random positions which are truly uniform over the sphere, with nothing enforced on tile scale.
Though, I have built in an option to still run with equal number per tile settings to be "backward compatible".
(This uses `--pertile`, but I don't recommend using this. Objects are placed into the "unique areas" defined by DESDM,
which are not exactly equal area, so one ends up with slightly non-uniform sampling.)

There is a file called [`BuildPos.py`](https://github.com/suchyta1/BalrogMPI/blob/master/BuildPos.py), which generates the positions.
Run `BuildPos.py --help` for the command line arguments. They should be relatively clear.
If you use the same `--seed`, `--density`/`--pertile`, (and `--iterateby` if using `--density`),
with the same file given in `--tiles`, you'll get the same positions. 
If you append to the `--tiles` file and run again, you'll ultimately generate `balrog_index` values which are consistent for the common tiles.

I haven't supplied a script to generate jobs for `BuildPos.py`, 
because you likely don't need to do this very often, and at any rate, it's not very complex. You'll want an `mpirun` (or `srun`, or whatever) something like below.
I use `mpi` because for `--density` sampling, generating points over the whole sphere can be a lot of points / use a lot of memory, 
so one iterates, and uses multiple machines/cores.
I could add code to only generate within the RA/DEC boundaries the tiles actually occupy, to make this more efficient, but I haven't done that yet.
At the moment, tiles which wrap around zero will confuse the code. I'll come up with a fix for this.

### Example

```
mpirun -np 150 -hostfile %hostfile% ./BuildPos.py --density 200000 --seed 100  \
--tiles /some/directory/spt-y1a1-only-g70-grizY.fits \
--outdir /somewhere/spt-y1a1-only-g70-grizY-pos-sphere/
```

## Generating the `Balrog` job

Use [`BuildJob.py`](https://github.com/suchyta1/BalrogMPI/blob/master/BuildJob.py) to build the `Balrog` jobs to submit to the queue scheduler.
In principle, these jobs have a practically infinite number of adjustable parameters, but for many of them I've set the defaults to be what you probably want.
You shouldn't need to ever look at a lot of the settings, so they're indeed hidden from you to start.

As I mentioned above, there are also a lot of dependencies which need to be set up. 
One can supply a bash script to be sourced to set up all the software at the computing site.
This is done with the `--source` command line argument.
(See [`site-setups/Edison/y1-source`](https://github.com/suchyta1/BalrogMPI/blob/master/site-setups/Edison/y1-source.sh) as an example I use on Edison.)
If `--source` isn't given, you'll need to have everything set up by default in your rc file.
To be explicitly clear, the file given will be sourced both when  you're running `BuildJob.py`,
and in your output job submission file. (The former is so it's easier to build your jobs from the command line, without requiring you to manually set things up.)

`BuildJob.py` takes up to 3 command line arguments.
`--source` was mentioned above.
You'll always need to give an argument to `--config`. This is a python file 
(see [`site-setups/Edison/y1-config.py`](https://github.com/suchyta1/BalrogMPI/blob/master/site-setups/Edison/y1-config.py) as an example I use on Edison), 
which sets up configuration dictionaries (except `tiles`, which is an array) for all the run parameters. You edit a function called `CustomConfig`.
Technically speaking all of the entries in the dictionaries have default values, and you're changing these,
but there's essentially no set of defaults which possibly actually makes sense.
(See [`source-code/RunConfigurations.py`](https://github.com/suchyta1/BalrogMPI/blob/master/source-code/RunConfigurations.py) for most of the defaults,
but some are intentionally more so hidden in [`/source-code/GenerateJob.py`](https://github.com/suchyta1/BalrogMPI/blob/master/source-code/GenerateJob.py).)

The third command line argument is `--scheduler`, which speicifies the work queue you're submitting to. 
Currently only `['slurm','wq']` are accepted. 
[`SLURM`](http://slurm.schedmd.com/documentation.html) is the schduler at NERSC,
and [`wq`](https://github.com/suchyta1/Valarauko#generating-the-balrog-job) is the scheduler at BNL.
If you're at BNL or NERSC you can forget this even exists, and the script will auto-detect what to do.
A working example on Edison would look something like:

### Example

```
./BuildJob.py --config site-setups/Edison/y1-config.py --source site-setups/Edison/y1-source.sh
```

### Need to Know Parameters

I've tried the make the names of the parameters understandable. The best way to get a feel for what's going on is to 
look at an example ([e.g. here](https://github.com/suchyta1/BalrogMPI/blob/master/site-setups/Edison/y1-config.py)) in the repository.
Some explanations are below. The `balrog` dictionary entries are command line arguments to Balrog.
You almost definitely don't need to worry about the `db` dictionary. Most things are part of `run`.

* `dbname` -- the DB tables you'll write to. You'll get tables with this name, appended with `['truth','sim','nosim']` (and empty `'des'`).
* `jobdir` -- where output job files write
* `outdir` -- where output temporary files (images, etc.) write. Set this to somewhere on the scratch disk.
* `pyconfig` -- the `Balrog` `--pyconfig` file.
* `ngal` -- number of `Balrog` objects per realization
* `pos` -- directory with positoin files generated for the `Balrog` run set from `BuildPos.py`
* `release` -- the release name of the coadd dataset from DESDM.
* `tiles` -- a list of DES coadd tilenames
* `nodes` -- how many nodes your job will use.
* `npersubjob` -- number of tiles to run on each node (in each job file). Tiles on the same node run sequentially.

`n = len(tiles)/(nodes*npersubjob)` must be an intger with `SLURM`, and equal to 1 if you're using `wq`.
If `n > 1`, then `n` dependent jobs will be submitted, where the subsequent jobs are dependent on the previous one's success.
(I'm thinking about removing the success stipulation.)


#### Dangerous (but can be useful)

* `DBoverwrite` -- overwrite the existing `dbname` if it already exists.

Also, unless you're running small test jobs, and you understand what you're doing, don't mess with the hidden cleaning parameters. 
You can easily fill entire disks if you do.

#### Extra needed inputs

* `db-columns` -- what fields to populate in the output DB. I'm getting this by describing the DESDM tables.
* Paths to the wanted versions of `balrog`, `sextractor`, `swarp`, `wget`, `funpack`. The default is try to find something in your `$PATH`.
* Configuration files for `sextractor` and `swarp`. Having defaults for these doesn't really make sense.


## What else?

If you look in [`aux`](https://github.com/suchyta1/Valarauko/tree/master/aux), you'll find
scripts that give an idea of how I've generated some of the auxilary needed inputs, such as the column names to use, or sets of tiles.
The [`tiles`](https://github.com/suchyta1/Valarauko/tree/master/tiles) directory has some tile lists.

Take a look at the [issues page](https://github.com/suchyta1/Valarauko/issues). These aren't really bugs, and knowing some of the issues can help you understand key points.
