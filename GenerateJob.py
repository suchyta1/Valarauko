#!/usr/bin/env python

import RunConfigurations
import os
import sys
import esutil
import ConfigureFunction
import AutoJob


def GenJob(argv):
    where = AutoJob.GetWhere(argv)
    run, balrog, db, tiles = ConfigureFunction.GetConfig(where)
    job = ConfigureFunction.Generate_Job(run, where)
    return job, where


if __name__ == "__main__":

    job, where = GenJob(sys.argv)
    print job
