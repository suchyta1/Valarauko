Updated README coming. There have been changes to more or less everything. I'll try to document once it's all finalized.

## Generating the Balrog job

Use [```BuildJob.py```](https://github.com/suchyta1/BalrogMPI/blob/nompi/BuildJob.py) to build the Balrog jobs to submit to the queue scheduler.
In principle, these jobs have a practically infinite number of adjustable parameters, but for many of them I've set the defaults to be what you probably want.
You shouldn't need to ever look at a lot of the settings, so they're indeed hidden from you to start.
There are also a lot of dependencies. (I have everything needed installed on the astro cluster at BNL and edison at NERSC.)
One can supply a bash script to be sourced to set up all the software at the computing site.
This is done with the ```--source``` command line argument.
(See [```site-setups/Edison/y1-source```](https://github.com/suchyta1/BalrogMPI/blob/nompi/site-setups/Edison/y1-config.py) as an example I use on edison.)
If ```--source``` isn't given, you'll need to have everything set up by default in your rc file.
To be explicitly clear, the file given will be sourced but when  you're running [```BuildJob.py```](https://github.com/suchyta1/BalrogMPI/blob/nompi/BuildJob.py) itself,
and in your job submission file. (The former is so it's easier to build your jobs from the command line, without requiring you to manually set things up.)

[```BuildJob.py```](https://github.com/suchyta1/BalrogMPI/blob/nompi/BuildJob.py) takes up to 3 command line arguments.
```--source``` was mentioned above.
You'll always need to give an argument to ```--config```. This is a python file 
(see [```site-setups/Edison/y1-config.py```](https://github.com/suchyta1/BalrogMPI/blob/nompi/site-setups/Edison/y1-config.py) as an example I use on edison), 
which sets up configuration dictionaries for all the run parameters.
You edit a function called ```CustomConfig```.
Technically speaking all of the entries in the dictionaries have default values, and you're changing these,
but there's essentially no set of defaults which possibly actually makes sense.
