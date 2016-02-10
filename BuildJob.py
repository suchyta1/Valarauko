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
    parser = argparse.ArgumentParser()

    parser.add_argument("-cl", "--cluster", help="Cluster you are (will be) running on", default=None, type=str)
    parser.add_argument("-co", "--config", help="Custom configuration file to use", required=True, type=str)
    parser.add_argument("-s", "--source", help="A file to source to setup any software needed to build/run the job", default=None, type=str)

    args = parser.parse_args()
    argslog = Printer()

    err = False
    clus = ['astro', 'edison', 'cori']
    if args.cluster is None:
        host = socket.gethostname()
        for h in clus:
            if host.lower().startswith(h):
                args.cluster = h
        argslog.warning("You didn't give --cluster, but I auto-dectected you're on %s. I'm using that."%(args.cluster))
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
    cmd = '%s%s %s %s %s'%(cmd, gen, args.cluster, args.config)
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
