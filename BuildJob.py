#!/usr/bin/env python

import argparse
import socket
import logging
import sys
import os
import subprocess


def TrustEric(run, where='edison'):
    if where.lower=='edison':
        run['ppn'] = 48
        run['cores'] = 48
        run['paralleldownload'] = True
        run['DBoverwrite'] =  False 
        run['duplicate'] = 'error'
        run['allfail'] = True
        run['asdependency'] = True
        run['email'] = None 
        run['queue'] = 'regular' # Which queue to use if running at NERSC. 
        run['stripe'] = 2


def Printer():
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.NOTSET)
    
    argslog = logging.getLogger('args')
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)
    argslog.addHandler(sh)
    return argslog


def GetArgs():
    parser = argparse.ArgumentParser()

    parser.add_argument("-sc", "--scheduler", help="Job scheduler you will be running with. slurm and wq are supported, and can be autodetected at NERSC and BNL.", default=None, type=str.lower, choices=['slurm','wq'])
    parser.add_argument("-co", "--config", help="Custom configuration file to use", required=True, type=str)
    parser.add_argument("-so", "--source", help="A file to source to setup any software needed to build/run the job", default=None, type=str)

    args = parser.parse_args()
    argslog = Printer()

    err = False
    known = [('astro','wq'), ('edison','slurm'), ('cori','slurm')]
    if args.scheduler is None:
        host = socket.gethostname()
        for i in range(len(known)):
            if host.lower().startswith(known[i][0]):
                args.scheduler = known[i][1]
                argslog.warning("You didn't give --scheduler, but I auto-dectected you're on %s. I'm using %s."%(known[i][0],args.scheduler))
                break
    if args.scheduler is None:
        argslog.error("You didn't give --scheduler, and I detected your host is %s. The only default setups I know are: %s. Tell me what to use with --scheduler"%(host, str(known)))
        err = True

    if not os.path.exists(args.config):
        argslog.error("--config given does not exist: %s."%(args.config))
        err = True
    args.config = os.path.realpath(args.config)

    if args.source is None:
        argslog.warning("You did not give a --source file. This means everything you need must be set up by default in your rc files.")
    else:
        if not os.path.exists(args.source):
            argslog.error("--source given does not exist: %s."%(args.source))
            err = True
        args.source = os.path.realpath(args.source)


    msg = 'No job file written'
    if err:
        argslog.error(msg)
        sys.exit()

    return args, argslog, msg



if __name__ == "__main__":
    args, argslog, msg = GetArgs()

    thisdir = os.path.dirname(os.path.realpath(__file__))
    gen = os.path.join(thisdir, 'source-code', 'GenerateJob.py')

    cmd = ''
    if args.source is not None:
        cmd = 'source %s && '%(args.source)
    cmd = '%s%s %s %s'%(cmd, gen, args.scheduler, args.config)
    if args.source is not None:
        cmd = '%s %s'%(cmd, args.source)

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    rcode = p.returncode

    if len(stderr) > 0:
        argslog.error(stderr)
        argslog.error(msg)
    elif rcode != 0:
        argslog.error(stdout.strip())
        argslog.error(msg)
    else:
        lines = stdout.strip().split('\n')
        for i in range(len(lines)):
            line = lines[i]
            if i==(len(lines)-1):
                argslog.info(line)
            else:
                argslog.warning(line)
