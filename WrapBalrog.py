#!/usr/bin/env python

import sys
import os
import numpy as np
import desdb
import copy

from mpi4py import MPI
import mpifunctions

#from RunConfigurations import *
import Configure
import runbalrog


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
    inc = 1000
    
    if RunConfiguration['fixposseed']!=None:
        np.random.seed(RunConfiguration['fixposseed'])

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



# Delete the existing DB tables for your run if the names already exist
def DropTablesIfNeeded(RunConfig, BalrogConfig, allbands=['det','g','r','i','z','Y']):
    cur = desdb.connect()
    user = cur.username

    '''
    tables = ['truth', 'sim']
    try:
        tmp = BalrogConfig['nonosim']
    except:
        tables.insert(1, 'nosim')
    if RunConfig['doDES']:
        tables.append('des')

    bands = runbalrog.PrependDet(RunConfig)
    '''

    write = allbands
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
                    write = None
                    if kind=='truth':
                        arr = cur.quick("select coalesce(max(balrog_index),-1) as max from %s"%(tab), array=True)
                        num = int(arr['max'][0])
                        if num > max:
                            max = num
    indexstart = max + 1
    return indexstart,write


def PrepareCreateOnly(tiles, images, psfs, position, config):
    return [tiles[0:1], images[0:1], psfs[0:1], [ [] ], [-2], [0]]


def PrepareIterations(tiles, images, psfs, position, config, RunConfig, indexstart):
    #sendpos = copy.copy(position)
    sendpos = []
    senditerations = []
    sendindexstart = []


    cur = desdb.connect()
    q = "select urall, uraur, udecll, udecur, tilename from coaddtile"
    all = cur.quick(q, array=True)
    cut = np.in1d(all['tilename'], tiles)
    dcoords = all[cut]


    for i in range(len(tiles)):
        iterations = np.ceil( len(pos[i]) / float(config['ngal']))

        #sendpos[i] = np.array_split(pos[i], iterations, axis=0)
        sendpos.append( [] )
        for j in range(int(iterations)):
            start = j * config['ngal']
            stop = start + config['ngal']
            sendpos[i].append( pos[i][start:stop] )

        if RunConfig['doDES']:
            senditerations.append(np.arange(-1, iterations, 1, dtype=np.int32))
            sendpos[i].insert(0, [])
        else:
            senditerations.append(np.arange(0, iterations, 1, dtype=np.int32))
        sendindexstart.append(indexstart)
        indexstart += len(pos[i])
    
    #sendpos = np.array(sendpos)
    return [tiles, images, psfs, sendpos, senditerations, sendindexstart]



if __name__ == "__main__":
    
    RunConfig, config, SheldonConfig, DBConfig, tiles = Configure.GetConfig()

    '''
    # These effect a whole run's behavior. That is they are higher level than a single Balrog call.
    RunConfig = RunConfigurations.default

    # This is configuring desdb to find the right files.
    SheldonConfig = desdbInfo.sva1_coadd

    # What tiles you want to Balrog
    tiles = TileLists.suchyta13[1:2]
    #tiles = TileLists.suchyta27

    # These get passes as command line arguments to Balrog. If you add too much it could mess things up.
    config = BalrogConfigurations.default

    # Info for connecting to the DB. You probably don't need to touch this.
    DBConfig = DBInfo.default
    '''

    # Call desdb to find the tiles we need to download and delete any existing DB tables which are the same as your run label.
    if MPI.COMM_WORLD.Get_rank()==0:
        images, psfs, tiles = GetFiles(RunConfig, SheldonConfig, tiles)
        indexstart, write = DropTablesIfNeeded(RunConfig, config)
    else:
        write = None
        tiles = None
    write = MPI.COMM_WORLD.allgather(write)[0]
    tiles = MPI.COMM_WORLD.allgather(tiles)[0]

    # Generate positions for the simulated objects
    pos = RandomPositions(RunConfig, config, tiles)


    # This creates the DB tables.
    # It will do a minimal Balrog run, intended only to get the output catalog columns, so we know what needs to be written to the DB.
    if write!=None:
        if MPI.COMM_WORLD.Get_rank()==0:
            ScatterStuff = PrepareCreateOnly(tiles, images, psfs, pos, config)
        else:
            ScatterStuff = [None]*6
        ScatterStuff = mpifunctions.Scatter(*ScatterStuff)
        for i in range(len(ScatterStuff[4])):
            DerivedConfig = {'tile': ScatterStuff[0][i],
                             'images': ScatterStuff[1][i],
                             'psfs': ScatterStuff[2][i],
                             'indexstart': ScatterStuff[5][i],
                             'iterations': [ScatterStuff[4][i]],
                             'pos': ScatterStuff[3],
                             'db': DBConfig}
            runbalrog.NewRunBalrog(RunConfig, config, DerivedConfig, write=write, nomulti=RunConfig['nomulti'])

    
    # This is all the real Balrog realizations.
    if MPI.COMM_WORLD.Get_rank()==0:
        ScatterStuff = PrepareIterations(tiles, images, psfs, pos, config, RunConfig, indexstart)
    else:
        ScatterStuff = [None]*6
    ScatterStuff = mpifunctions.Scatter(*ScatterStuff)
    for i in range(len(ScatterStuff[4])):
        DerivedConfig = {'tile': ScatterStuff[0][i],
                         'images': ScatterStuff[1][i],
                         'psfs': ScatterStuff[2][i],
                         'indexstart': ScatterStuff[5][i],
                         'iterations': ScatterStuff[4][i],
                         'pos': ScatterStuff[3][i],
                         'db':DBConfig}
        runbalrog.NewRunBalrog(RunConfig, config, DerivedConfig, nomulti=RunConfig['nomulti'])


    # Send email when the run finishes
    MPI.COMM_WORLD.barrier()
    if MPI.COMM_WORLD.Get_rank()==0:
        SendEmail(RunConfig)
