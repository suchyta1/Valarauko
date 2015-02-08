#!/usr/bin/env python

import sys
import os
import numpy as np
import desdb
import copy
import Queue
import socket
import logging
import subprocess

from mpi4py import MPI
import mpifunctions

import ConfigureFunction
import RunBalrog as runbalrog


"""
This is kind of cool, so you can send an email to yourself when the run finishes.
You'll notice my email in there, so if you're going to use this change the sender/reciever to yourself.
"""
def SendEmail(config):
    import smtplib
    from email.mime.text import MIMEText
    
    sender = 'eric.d.suchyta@gmail.com'
    receivers = [sender]
    msg = MIMEText( "Balrog run %s finished. \n \n--Message automatically generated by Balrog." %(config['label']) )
    msg['Subject'] = '%s completed' %(config['label'])
    msg['From'] = sender
    msg['To'] = sender
    
    obj = smtplib.SMTP('localhost')
    obj.sendmail(sender, receivers, msg.as_string())


# Find the files from the DES server we need to download
def GetFiles(RunConfig, SheldonConfig, tiles):

    #First get all runs
    bands = RunConfig['bands']
    runs = np.array( desdb.files.get_release_runs(SheldonConfig['release'], withbands=bands) )
    bands = runbalrog.PrependDet(RunConfig)
    kwargs = {}
    kwargs['type'] = SheldonConfig['filetype']
    kwargs['fs'] = 'net'

    keepruns = []
    keepimages = []
    keeppsfs = []
    keeptiles = []

    # Pick out only the tiles we wanted
    for i in range(len(runs)):
        run = runs[i]
        tile = run[-12:]
        if tile in tiles:
            keeptiles.append(tile)
            keepruns.append(run)
            keepimages.append( [] )
            keeppsfs.append( [] )
            kwargs[SheldonConfig['runkey']] = run
            kwargs['tilename'] = tile

            for band in bands:
                kwargs['band'] = band
                image = desdb.files.get_url(**kwargs)
                image = image.replace('7443','')
                psf = image.replace('.fits.fz', '_psfcat.psf')
                keepimages[-1].append(image)
                keeppsfs[-1].append(psf)

    return [keepimages, keeppsfs, keeptiles]    


# Generate random, unclustered object positions
def RandomPositions(RunConfiguration, BalrogConfiguration, tiles, seed=None):

    # Find the unique tile area, only simulate into the unique area since other area will get cut anyway
    cur = desdb.connect()
    q = "select urall, uraur, udecll, udecur, tilename from coaddtile"
    all = cur.quick(q, array=True)
    cut = np.in1d(all['tilename'], tiles)
    dcoords = all[cut]

    # To make generating the sample easier, I simulate within bounds [ramin, ramax], [decmin,decmax] then only take what's in the tiles we're using
    ramin = np.amin(dcoords['urall'])
    ramax = np.amax(dcoords['uraur'])
    decmin = np.amin(dcoords['udecll'])
    decmax = np.amax(dcoords['udecur'])

    # Put into right range for arccos
    if decmin < 0:
        decmin = 90 - decmin
    if decmax < 0:
        decmax = 90 - decmax


    wcoords = []
    for tile in tiles:
        wcoords.append( np.empty( (0,2) ) )

    target = len(tiles) * RunConfiguration['tiletotal'] / float(MPI.COMM_WORLD.size)
    #inc = 10000 
    inc = RunConfiguration['inc']
    
    if RunConfiguration['fixposseed']!=None:
        rank = MPI.COMM_WORLD.Get_rank()
        np.random.seed(RunConfiguration['fixposseed'] + rank)

    # Loop until we've kept at least as many galaxies as we wanted, we'll generate inc at a time and see which of those lie in our tiles
    numfound = 0
    while numfound < target:
        ra = np.random.uniform(ramin,ramax, inc)
        dec = np.arccos( np.random.uniform(np.cos(np.radians(decmin)),np.cos(np.radians(decmax)), inc) ) * 180.0 / np.pi
        neg = (dec > 90.0)
        dec[neg] = 90.0 - dec[neg]
        coords = np.dstack((ra,dec))[0]
    
        for i in range(len(tiles)):

            tilecut = (dcoords['tilename']==tiles[i])
            c = dcoords[tilecut][0]
            inside = (ra > c['urall']) & (ra < c['uraur']) & (dec > c['udecll']) & (dec < c['udecur'])
          
            found = np.sum(inside)
            if found > 0:
                wcoords[i] = np.concatenate( (wcoords[i],coords[inside]), axis=0)
            numfound += found

    for i in range(len(wcoords)):
        wcoords[i] = mpifunctions.Gather(wcoords[i])

    return wcoords


def GetAllBands():
    return ['det','g','r','i','z','Y']

# Delete the existing DB tables for your run if the names already exist
def DropTablesIfNeeded(RunConfig, BalrogConfig):
    allbands = GetAllBands()
    cur = desdb.connect()
    user = cur.username

    write = True
    max = -1
    arr = cur.quick("select table_name from dba_tables where owner='%s'" %(user.upper()), array=True)
    tables = arr['table_name']
    for band in allbands:
        for  kind in ['truth', 'nosim', 'sim', 'des']:
            tab = 'balrog_%s_%s_%s' %(RunConfig['label'], kind, band)
            if tab.upper() in tables:
                if RunConfig['DBoverwrite']:
                    cur.quick("DROP TABLE %s PURGE" %tab)
                else:
                    write = False
                    if kind=='truth':
                        arr = cur.quick("select coalesce(max(balrog_index),-1) as max from %s"%(tab), array=True)
                        num = int(arr['max'][0])
                        if num > max:
                            max = num
    indexstart = max + 1
    return indexstart,write



def GetQueueItem(queue, hostinfo, host, rank):
    hostinfo[host] = queue.get()
    hostinfo[host]['waiting'] = []
    hostinfo[host]['done'] = []
    hostinfo[host]['initialized'] = -1
    hostinfo[host]['tiletodo'] = hostinfo[host]['it'].qsize()
    hostinfo[host]['tileits'] = 0
    return hostinfo


def TileDone(hostinfo, host):
    if hostinfo[host]['tileits']==hostinfo[host]['tiletodo']:
        return True
    else:
        return False


def StartWaiting(hostinfo, host, rank, ServerLog, descr):
    hostinfo[host]['waiting'].append(rank)
    tile = hostinfo[host]['balrog']['tile']
    job = ['wait', tile, descr]
    ServerLog.Log('Telling process to wait, waiting for tile = %s to %s'%(tile, descr) )
    MPI.COMM_WORLD.send(job, dest=rank)


def ResumeWaiting(hostinfo, host, ServerLog):
    for w in hostinfo[host]['waiting']:
        ServerLog.Log('Telling process to stop waiting')
        MPI.COMM_WORLD.send('go', dest=w)
        ServerLog.Log('Stopped waiting')
    hostinfo[host]['waiting'] = []
    return hostinfo


def Shutdown(hostinfo, host, rank, done, RunConfig, ServerLog):
    job = ['shutdown', 0, 0]
    ended = MPI.COMM_WORLD.sendrecv(job, dest=rank, source=rank)
    done += 1

    if host in hostinfo.keys():
        hostinfo = ResumeWaiting(hostinfo, host, ServerLog)
        hostinfo[host]['done'].append(rank)

    return done


def Cleanup(RunConfig, rank, host, ServerLog, hostinfo):
    workingdir = hostinfo[host]['derived']['workingdir']
    job = ['cleanup', RunConfig['tile-clean'], workingdir]
    ServerLog.Log('Sending signal to do any cleanup for tile = %s' %(hostinfo[host]['balrog']['tile']) )
    MPI.COMM_WORLD.sendrecv(job, dest=rank, source=rank)
    ServerLog.Log('Done doing any cleanup for tile = %s' %(hostinfo[host]['balrog']['tile']) )


class ServerLogger(object):

    def __init__(self, log, startstr):
        self.startstr = startstr
        self.log = log

    def Log(self, msg):
        self.log.info(self.startstr + msg)


def ServeProcesses(queue, RunConfig, logdir, desdblogdir):
    size = RunConfig['nodes'] * RunConfig['ppn'] - 1
    done = 0
    hostinfo = {}

    thisrank = MPI.COMM_WORLD.Get_rank()
    thishost = socket.gethostname()
    log = SetupLog(logdir, thishost, thisrank)
    log.info('Started server')

    while done < size:
        obj_recv = MPI.COMM_WORLD.recv(source=MPI.ANY_SOURCE)
        rank, host, code = obj_recv

        startstr = 'workhost = %s, workrank = %i '%(host, rank)
        ServerLog = ServerLogger(log, startstr)
        ServerLog.Log('Received a signal')


        if code >= -2:
            hostinfo[host]['tileits'] += 1

        if code in [-3,-2]:
            hostinfo[host]['initialized'] = 1
            hostinfo = ResumeWaiting(hostinfo, host, ServerLog)
            if code==-3:
                MPI.COMM_WORLD.send('continue', dest=rank)
                continue

        if host not in hostinfo.keys():
            log.info('Received signal from unadded host = %s, rank = %i' %(host,rank))
            if queue.qsize() > 0:
                hostinfo = GetQueueItem(queue, hostinfo, host, rank)
                ServerLog.Log( 'Got queue for tile = %s' %(hostinfo[host]['balrog']['tile']) )
            else:
                ServerLog.Log( 'Node rank higher than number of tiles, not needed' )
                ServerLog.Log( 'Sending signal to shut down' )
                done = Shutdown(hostinfo, host, rank, done, RunConfig, ServerLog)
                ServerLog.Log('Shut down')
                continue


        if hostinfo[host]['it'].qsize()==0:
            ServerLog.Log( 'Received signal from empty queue, tile = %s' %(hostinfo[host]['balrog']['tile']) )
            if TileDone(hostinfo, host):
                Cleanup(RunConfig, rank, host, ServerLog, hostinfo)
                ResumeWaiting(hostinfo, host, ServerLog)
                if queue.qsize() > 0:
                    hostinfo = GetQueueItem(queue, hostinfo, host, rank)
                    ServerLog.Log( 'Got new queue for tile = %s' %(hostinfo[host]['balrog']['tile']) )
                    #ResumeWaiting(hostinfo, host, ServerLog)
                else:
                    ServerLog.Log( 'tile = %s has empty queue' %(hostinfo[host]['balrog']['tile']) )
                    log.info('Sending signal to shut down')
                    done = Shutdown(hostinfo, host, rank, done, RunConfig, ServerLog)
                    log.info('Shut down')
                    continue
            else:
                StartWaiting(hostinfo, host, rank, ServerLog, 'finish')
                continue

        if hostinfo[host]['initialized']==0:
            StartWaiting(hostinfo, host, rank, ServerLog, 'initialize')
            continue
        elif hostinfo[host]['initialized']==-1:
            hostinfo[host]['initialized'] = 0


        run = copy.copy(RunConfig)
        balrog = copy.copy(hostinfo[host]['balrog'])
        derived = copy.copy(hostinfo[host]['derived'])

        derived['iteration'] = hostinfo[host]['it'].get()
        it = runbalrog.EnsureInt(derived)
        derived['pos'] = hostinfo[host]['pos'].get()
        derived['initialized'] = bool( hostinfo[host]['initialized'] )
        derived['outdir'] = os.path.join(derived['workingdir'], 'output', '%i'%it)

        #print derived['iteration'], hostinfo[host]['tileits'], hostinfo[host]['initialized'], balrog['tile']
        balrog['indexstart'] = derived['indexstart']
        if it > 0:
            balrog['indexstart'] += it*balrog['ngal']
            balrog['ngal'] = len(derived['pos'])
        balrog['seed'] = balrog['indexstart'] + derived['seedoffset']

        job = [RunConfig, balrog, derived]
        if derived['iteration']==-2:
            derived['bands'] = GetAllBands()
            ServerLog.Log('Initializing DB via tile = %s' %(balrog['tile']) )
            started = MPI.COMM_WORLD.sendrecv(job, dest=rank, source=rank)
            ServerLog.Log('Done Initializing DB via tile = %s' %(balrog['tile']) )
        else:
            derived['bands'] = runbalrog.PrependDet(RunConfig)
            ServerLog.Log('Doing tile = %s, it = %s' %(balrog['tile'], str(derived['iteration'])) )
            MPI.COMM_WORLD.send(job, dest=rank)


def SetupLog(logdir, host, rank):
    logfile = os.path.join(logdir, '%i.log'%(rank))
    log = logging.getLogger('rank')
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s -  %(hostname)s , %(ranknumber)s - %(message)s')
    fh = logging.FileHandler(logfile, mode='w')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    log.addHandler(fh)

    extra = {'hostname': 'host = %s'%host,
             'ranknumber': 'rank = %i'%rank}
    log = logging.LoggerAdapter(log, extra)
    return log


def DoProcesses(logdir, desdblogdir):

    rank = MPI.COMM_WORLD.Get_rank()
    host = socket.gethostname()

    log = SetupLog(logdir, host, rank)
    log.info('Started listener')
    dlog = os.path.join(desdblogdir, '%i.log'%(rank))

    send = -7
    while True:
        log.info('Ready for job')
        job = MPI.COMM_WORLD.sendrecv( [rank,host,send], dest=0, source=0)
        
        if job[0]=='shutdown':
            send = -5
            log.info('Shutting down')
            MPI.COMM_WORLD.send(send, dest=0)
            log.info('Shut down')
            break
        elif job[0]=='cleanup':
            send = -6
            clean = job[1]
            workingdir = job[2]
            if clean and os.path.lexists(workingdir):
                log.info('cleaning up')
                #subprocess.call( ['rm', '-r', workingdir] )
                oscmd = ['rm', '-r', workingdir]
                runbalrog.SystemCall(oscmd)
                log.info('removed %s' %(workingdir) )
            #MPI.COMM_WORLD.send(send, dest=0)
        elif job[0]=='wait':
            send = -4
            log.info('Waiting for tile = %s to %s' %(job[1], job[2]) )
            go = MPI.COMM_WORLD.recv(source=0)
            log.info('Resuming, tile = %s %s completed' %(job[1], job[2]))

        else:
            run, balrog, derived = job
            derived['desdblog'] = dlog
            send = runbalrog.EnsureInt(derived)
            log.info('Running iteration = %s of tile = %s' %(str(derived['iteration']),balrog['tile']) )
            runbalrog.MPIRunBalrog(run, balrog, derived)
            if derived['iteration']==-2:
                MPI.COMM_WORLD.send(send, dest=0)
                log.info('Initialized tile = %s' %(balrog['tile']) )



def BuildQueue(tiles, images, psfs, pos, BalrogConfig, RunConfig, dbConfig, indexstart, write):
    fullQ = Queue.Queue(len(tiles))

    for i in range(len(tiles)):
        derived, balrog = InitCommonToTile(images[i], psfs[i], indexstart, RunConfig, BalrogConfig, dbConfig, tiles[i])

        workingdir = os.path.join(RunConfig['outdir'], RunConfig['label'], balrog['tile'] )
        derived['workingdir'] = workingdir
        derived['indir'] = os.path.join(workingdir, 'input')

        #iterations = (len(pos[i]) / balrog['ngal']) + (len(pos[i]) % balrog['ngal'])
        #iterations = np.ceil( len(pos[i]) / float(balrog['ngal']))
        iterations = len(pos[i]) / balrog['ngal']
        if ( len(pos[i]) % balrog['ngal'] ) != 0:
            iterations += 1

        size = iterations

        if i==0 and write:
            size += 1
            
        if RunConfig['doDES']:
            size += 1

        itQ = Queue.Queue(size)
        posQ = Queue.Queue(size)

        if i==0 and write:
            itQ.put(-2)
            posQ.put(None)

        if RunConfig['doDES']:
            for k in range(len(band)):
                itQ.put( (-1, k) )
                posQ.put(None)

        for j in range(int(iterations)):
            start = j * balrog['ngal']
            if j==(iterations-1):
                stop = len(pos[i])
            else:
                stop = start + balrog['ngal']
            itQ.put(j)
            posQ.put(pos[i][start:stop])
    
        d = {'derived': derived,
             'balrog': balrog,
             'pos': posQ,
             'it': itQ}

        fullQ.put(d)
        indexstart += len(pos[i])
    
    return fullQ


def InitCommonToTile(images, psfs, indexstart, RunConfig, BalrogConfig, dbConfig, tile):
        derived = {'images': images,
                   'psfs': psfs,
                   'indexstart': indexstart,
                   'db': dbConfig}
        if RunConfig['fixwrapseed'] != None:
            derived['seedoffset'] = RunConfig['fixwrapseed']
        else:
            derived['seedoffset'] = np.random.randint(10000)

        balrog = copy.copy(BalrogConfig)
        balrog['tile'] = tile

        return derived, balrog



if __name__ == "__main__":
    
    RunConfig, BalrogConfig, desdbConfig, dbConfig, tiles = ConfigureFunction.GetConfig(sys.argv[1])

    runlogdir = 'runlog-%s-%s' %(RunConfig['label'], RunConfig['joblabel'])
    commlogdir = os.path.join(runlogdir, 'communication')
    desdblogdir = os.path.join(runlogdir, 'desdb')

    # Call desdb to find the tiles we need to download and delete any existing DB tables which are the same as your run label.
    if MPI.COMM_WORLD.Get_rank()==0:
        images, psfs, tiles = GetFiles(RunConfig, desdbConfig, tiles)
        indexstart, write = DropTablesIfNeeded(RunConfig, BalrogConfig)
    else:
        tiles = None
    tiles = MPI.COMM_WORLD.allgather(tiles)[0]

    # Generate positions for the simulated objects
    pos = RandomPositions(RunConfig, BalrogConfig, tiles)

    if MPI.COMM_WORLD.Get_rank()==0:
        if os.path.exists(runlogdir):
            oscmd = ['rm', '-r', runlogdir]
            runbalrog.SystemCall(oscmd)
        runbalrog.Mkdir(commlogdir)
        runbalrog.Mkdir(desdblogdir)
    MPI.COMM_WORLD.barrier()
    
    if MPI.COMM_WORLD.Get_rank()==0:
        q = BuildQueue(tiles, images, psfs, pos, BalrogConfig, RunConfig, dbConfig, indexstart, write)
        ServeProcesses(q, RunConfig, commlogdir, desdblogdir)
    else:
        DoProcesses(commlogdir, desdblogdir) 


    # Send email when the run finishes
    MPI.COMM_WORLD.barrier()
    if MPI.COMM_WORLD.Get_rank()==0:
        SendEmail(RunConfig)
