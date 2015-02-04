#!/usr/bin/env python

import GenerateJob
import subprocess
import sys

def GetWhere(argv):
    if len(argv) > 1:
        where = argv[1]
    else:
        where = 'BNL'
    return where


if __name__ == "__main__":

    job, where = GenerateJob.GenJob(sys.argv)

    if where=='BNL':
        subprocess.call( ['wq', 'sub', '-b', job] )

    elif where=='NERSC':
        subprcoess.call( ['qsub', job] )
