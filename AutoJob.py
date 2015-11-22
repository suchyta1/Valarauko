#!/usr/bin/env python

import GenerateJob
import subprocess
import sys


if __name__ == "__main__":

    job, where = GenerateJob.GenJob(sys.argv)

    if where=='BNL':
        subprocess.call( ['wq', 'sub', '-b', job] )

    elif where=='NERSC':
        subprcoess.call( ['qsub', job] )
