#!/usr/bin/env python

import argparse
import socket
import logging
import sys
import os
import subprocess


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
    descr = """--asarray has no effect at BNL; there's no concept of job arrays with wq.
    At least for now, you probably don't want to use --npersubjob at BNL either, because the subjobs are launched sequentially.
    Concurrence might be doable; I have to play around with how the hosts are specified / if wait works.
    At NERSC, subjobs launch in parallel."""
    parser = argparse.ArgumentParser(description=descr)

    parser.add_argument("-cl", "--cluster", help="Cluster you are (will be) running on", default=None, type=str)
    parser.add_argument("-co", "--config", help="Custom configuration file to use", required=True, type=str)
    parser.add_argument("-s", "--source", help="A file to source to setup any software needed to build/run the job", default=None, type=str)
    parser.add_argument("-d", "--dir", help="Directory where to output your run directory", default=None, type=str)

    parser.add_argument("-npsj", "--npersubjob", help="Number of tiles per subjob. <=0 or >=(number of tiles) means whole job in one call.", default=0, type=int)
    parser.add_argument("-a", "--asarray", help="Write job as a job array. This launches subjobs as separate jobs.", action="store_true")


    args = parser.parse_args()
    argslog = Printer()

    err = False
    clus = ['astro', 'edison', 'cori']
    if args.cluster is None:
        host = socket.gethostname()
        for h in clus:
            if host.lower().startswith(h):
                args.cluster = h
        argslog.info("You didn't give --cluster, but I auto-dectected you're on %s. I'm using that."%(args.cluster))
    if args.cluster is None:
        argslog.error("You didn't give --cluster, and I detected your host is %s. That's not one of the known ones I can use from auto-detecting: %s. Tell me what to use with --cluster"%(host, str(clus)))
        err = True
    args.cluser = args.cluster.lower()
    if args.cluser not in clus:
        argslog.error("Invalid choice for --cluster: %s. Choose from %s"%(args.cluster,str(clus)))
        err = True
    if args.cluster=='astro':
        args.cluster = 'bnl'

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

    if args.dir is None:
        args.dir = os.path.dirname(os.path.realpath(__file__))
        argslog.info('No --dir given, setting it to: %s'%(args.dir))
    if not os.path.exists(args.dir):
        try:
            os.makedirs(args.dir)
        except:
            argslog.error('Could not make --dir %s'%(args.dir))
            err = True

    if args.asarray:
        args.usearray = 1
    else:
        args.usearray = 0

    if err:
        argslog.error('No job file written')
        sys.exit()

    return args, argslog



if __name__ == "__main__":
    args, argslog = GetArgs()

    thisdir = os.path.dirname(os.path.realpath(__file__))
    gen = os.path.join(thisdir, 'source-code', 'GenerateJob.py')

    cmd = ''
    if args.source is not None:
        cmd = 'source %s && '%(args.source)
    cmd = '%s%s %s %s %s %i %i'%(cmd, gen, args.cluster, args.config, args.dir, args.npersubjob, args.usearray)
    if args.source is not None:
        cmd = '%s %s'%(cmd, args.source)

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    if len(stderr) > 0:
        argslog.error(stderr)
    else:
        lines = stdout.strip().split('\n')
        for i in range(len(lines)):
            line = lines[i]
            if i==(len(lines)-1):
                argslog.info(line)
            else:
                argslog.warning(line)
