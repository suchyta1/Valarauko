#!/usr/bin/env python

import cx_Oracle
import copy
import time
import datetime
import StringIO
import sys
import os
import re
import subprocess
from multiprocessing import Pool, cpu_count, Lock
import argparse
import threading
#import pyfits
import astropy.io.fits as pyfits
import desdb
import numpy as np
import numpy.lib.recfunctions as recfunctions




def Remove(file):
    if os.path.lexists(file):
        os.remove(file)

def Mkdir(dir):
    if not os.path.lexists(dir):
        os.makedirs(dir)


def DownloadImages(indir, images, psfs, skip=False):
    useimages = []

    for file in images:
        infile = os.path.join(indir, os.path.basename(file))
        if not skip:
            Remove(infile)
            subprocess.call( ['wget', '-q', '--no-check-certificate', file, '-O', infile] )
        ufile = infile.replace('.fits.fz', '.fits')
        if not skip:
            Remove(ufile) 
            subprocess.call(['funpack', '-O', ufile, infile])
        useimages.append(ufile)

    usepsfs = []
    for psf in psfs:
        pfile = os.path.join(indir, os.path.basename(psf))
        if not skip:
            Remove(pfile)
            subprocess.call( ['wget', '-q', '--no-check-certificate', psf, '-O', pfile] )
        usepsfs.append(pfile)

    return [useimages, usepsfs]


def Dict2Cmd(d, cmd):
    l = [cmd]
    for key in d.keys():
        if type(d[key])==bool:
            if d[key]:
                l.append('--%s' %key)
        else:
            l.append('--%s' %key)
            l.append(str(d[key]))
    return l


def PrependDet(RunConfig):
    bands = copy.copy(RunConfig['bands'])
    if RunConfig['dualdetection']!=None:
        bands.insert(0, 'det')
    return bands


def GetDetStuff(BalrogConfig, RunConfig, images, ext=0, zpkey='SEXMGZPT', doprint=False):
    index = np.array( RunConfig['dualdetection'] )
    bands =  np.array(RunConfig['bands'])
    BalrogConfig['detbands'] = ','.join(bands[index] )
    
    zps = []
    inc = 0
    for i in index:
        num = i + 1
        header = pyfits.open(images[num])[ext].header
        zp = header[zpkey]

        #zps.append(str(zp))
        zps.append(zp)

        '''
        if inc==0:
            zpnew = zp
        else:
            zpnew += 2.5 * np.log10(1 + np.power(10.0, (zp-zpnew)/2.5))
        inc += 1

        if doprint:
            print num, images[num], zp
        '''

    #BalrogConfig['zeropoint'] = np.average(zps)
    BalrogConfig['zeropoint'] = np.amin(zps)
    for i in range(len(zps)):
        zps[i] = str(zps[i])

    BalrogConfig['detzeropoints'] = ','.join(zps)
    if doprint:
        print BalrogConfig['detzeropoints'], BalrogConfig['detbands'], BalrogConfig['zeropoint'], BalrogConfig['band']

    return BalrogConfig


def DoBandStuff(BalrogConfig, RunConfig, band, images, ext=0, zpkey='SEXMGZPT', doprint=False):
    BalrogConfig['band'] = band
    if band=='det':
        BalrogConfig = GetDetStuff(BalrogConfig, RunConfig, images, ext=ext, zpkey=zpkey, doprint=doprint)
    return BalrogConfig


'''
def GetSeed(BalrogConfig, DerivedConfig):
    BalrogConfig['seed'] = BalrogConfig['indexstart'] + DerivedConfig['seedoffset']
    return BalrogConfig
'''


def GetRelevantCatalogs(BalrogConfig, RunConfig, DerivedConfig, band=None):
    it = EnsureInt(DerivedConfig)
    if band==None:
        band = BalrogConfig['band']

    out_truth = os.path.join(BalrogConfig['outdir'], 'balrog_cat', '%s_%s.truthcat.sim.fits'%(BalrogConfig['tile'],band))
    out_nosim = os.path.join(BalrogConfig['outdir'], 'balrog_cat', '%s_%s.measuredcat.nosim.fits'%(BalrogConfig['tile'],band))
    out_sim = os.path.join(BalrogConfig['outdir'], 'balrog_cat', '%s_%s.measuredcat.sim.fits'%(BalrogConfig['tile'],band))

    relevant = {'truth': True, 'nosim': True, 'sim': True}

    if 'nonosim' in BalrogConfig.keys():
        if BalrogConfig['nonosim']:
            relevant['nosim'] = False
    if it==-1:
        relevant['truth'] = False
        relevant['nosim'] = False

    files = []
    labels = []
    if relevant['truth']:
        files.append(out_truth)
        labels.append('truth')
    if relevant['nosim']:
        files.append(out_nosim)
        labels.append('nosim')
    if relevant['sim']:
        files.append(out_sim)
        labels.append('sim')

    if it==-1:
        labels[-1] = 'des'

    elif it==-2:
        if RunConfig['doDES']:
            files.append(out_sim)
            labels.append('des')

    return files, labels


def EnsureInt(DerivedConfig):
    it = DerivedConfig['iteration']
    if type(it)==tuple:
        it = -1
    return it


def Number2NumberSex(ndata):
    names = np.array( ndata.dtype.names )
    dtype = []
    for i in range(len(names)):
        if names[i]=='NUMBER':
            dt = ('NUMBER_SEX', ndata.dtype.descr[i][1])
        else:
            dt = ndata.dtype.descr[i]
        dtype.append(dt)
    ndata.dtype = dtype
    return ndata


def VecAssoc2BalrogIndex(header, ndata, label, index_key='balrog_index'):
    pos = None
    for name in header.keys():
        if header[name] == index_key:
            pos = int(name[1:])
            break
    if pos!=None:
        if label!='des':
            index = ndata['VECTOR_ASSOC'][:, pos]
            ndata = recfunctions.append_fields(ndata, index_key, index, usemask=False)
        ndata = recfunctions.drop_fields(ndata, 'VECTOR_ASSOC', usemask=False)
    return ndata


def NewMakeOracleFriendly(file, ext, BalrogConfig, DerivedConfig, label):
    hdu = pyfits.open(file)[ext]
    header = hdu.header
    ndata = np.array(hdu.data)

    if label in ['nosim', 'sim', 'des']:
        ndata = Number2NumberSex(ndata)
        if ((label!='des') or (DerivedConfig['iteration']==-2)) and (('noassoc' not in BalrogConfig.keys()) or (BalrogConfig['noassoc']==False)):
            ndata = VecAssoc2BalrogIndex(header, ndata, label)
        
    t = np.array( [BalrogConfig['tile']]*len(ndata) )
    ndata = recfunctions.append_fields(ndata, 'tilename', t, '|S12', usemask=False)
    return ndata

def get_sqlldr_connection_info(db_specs):
    cur = desdb.connect()
    return '%s/%s@"(DESCRIPTION=(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%s))(CONNECT_DATA=(SERVER=%s)(SERVICE_NAME=%s)))"' %(cur.username,cur.password,db_specs['protocol'],db_specs['db_host'],db_specs['port'],db_specs['server'],db_specs['service_name'])


def NewWrite2DB(cats, labels, RunConfig, BalrogConfig, DerivedConfig):
    it = EnsureInt(DerivedConfig)
    create = False
    if it==-2:
        create = True

    cur = desdb.connect()
    for i in range(len(cats)):
        ext = 1
        if labels[i]!='truth' and BalrogConfig['catfitstype']=='ldac':
            ext = 2

        cat = cats[i]
        tablename = '%s.balrog_%s_%s_%s' %(cur.username, RunConfig['label'], labels[i], BalrogConfig['band'])
        arr = NewMakeOracleFriendly(cats[i], ext, BalrogConfig, DerivedConfig, labels[i])


        if RunConfig['DBload']=='sqlldr':
            controlfile = cat.replace('.fits', '')
            csvfile = cat.replace('.fits', '.csv')
            desdb.array2table(arr, tablename, controlfile, create=create)

            if create:
                create_file = '%s.create.sql' %(controlfile)
                create_cmd = open(create_file).read().strip()
                cur.quick(create_cmd)
            else:
                connstr = get_sqlldr_connection_info(DerivedConfig['db'])
                logfile = controlfile + '.sqlldr.log'
                subprocess.call(['sqlldr', '%s' %(connstr), 'control=%s' %(controlfile), 'log=%s' %(logfile), 'silent=(header, feedback)'])


        elif RunConfig['DBload']=='cx_Oracle':
            noarr = False
            if labels[i]=='truth':
                noarr = True

            if create:
                create_cmd = GetOracleStructure(arr, tablename, noarr=noarr, create=True)
                cur.quick(create_cmd)
            else:
                istr, newarr = GetOracleStructure(arr, tablename, noarr=noarr)
                cxcur, con = get_cx_oracle_cursor(DerivedConfig['db'])
                cxcur.prepare(istr)
                cxcur.executemany(None, newarr)
                con.commit()
                cxcur.close()
                #cxcur.executemany(istr, newarr)

        if create:
            cur.quick("GRANT SELECT ON %s TO DES_READER" %tablename)



def get_cx_oracle_cursor(db_specs):
    c = desdb.connect()
    connection = cx_Oracle.connect( "%s/%s@(DESCRIPTION=(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%s))(CONNECT_DATA=(SERVER=%s)(SERVICE_NAME=%s)))" %(c.username,c.password,db_specs['protocol'],db_specs['db_host'],db_specs['port'],db_specs['server'],db_specs['service_name']) )
    cur = connection.cursor()
    return cur, connection

def MakeNewArray(alldefs, arr, tablename, noarr=False):
    lists = []
    cols = []
    names = []
    nums = []
    for i in range(len(alldefs)):
        name = alldefs[i][0]
        if noarr:
            isarr = None
        else:
            isarr = re.search(r'_\d+$', name)

        if isarr==None:
            cols.append("arr['%s']"%(name) )
            lists.append( (arr[name]).tolist() )
        else:
            n = name[ : (isarr.span()[0]) ]
            j = int( isarr.group(0)[1:] ) - 1
            cols.append("arr['%s'][:,%i]"%(n,j) )
            lists.append( (arr[n][:,j]).tolist() )
        names.append(name)
        nums.append(':%i'%(i+1) )
    colstr = ', '.join(cols)
    numstr = ', '.join(nums)
    namestr = ', '.join(names)
    newarr = zip(*lists)
    istr = "insert into %s (%s) values (%s)" %(tablename, namestr, numstr)

    return istr, newarr


def GetOracleStructure(arr, tablename, noarr=False, create=False):
    a = arr.view(np.ndarray)
    cs, alldefs = desdb.get_tabledef(a.dtype.descr, tablename)

    if create:
        return cs
    else:
        istr, newarr = MakeNewArray(alldefs, arr, tablename, noarr=noarr)
        return istr, newarr


def WriteCoords(coords, outdir):
    coordfile = os.path.join(outdir, 'coords.fits')
    rcol = pyfits.Column(name='ra', format='D', unit='deg', array=coords[:,0])
    dcol = pyfits.Column(name='dec', format='D', unit='deg', array=coords[:,1])
    columns = [rcol, dcol]
    tbhdu = pyfits.BinTableHDU.from_columns(pyfits.ColDefs(columns))
    phdu = pyfits.PrimaryHDU()
    hdus = pyfits.HDUList([phdu,tbhdu])
    Remove(coordfile)
    hdus.writeto(coordfile)
    return coordfile


def RunOnlyCreate(RunConfig, BalrogConfig, DerivedConfig):
    BalrogConfig['ngal'] = 0
    BalrogConfig['image'] = DerivedConfig['images'][0]
    BalrogConfig['psf'] = DerivedConfig['psfs'][0]
    BalrogConfig = DoBandStuff(BalrogConfig, RunConfig, DerivedConfig['bands'][0], DerivedConfig['images'])
    BalrogConfig['outdir'] = os.path.join(DerivedConfig['outdir'], BalrogConfig['band'])

    cmd = Dict2Cmd(BalrogConfig, RunConfig['balrog'])
    subprocess.call(cmd)

    fixband = BalrogConfig['band']
    for i in range(len(DerivedConfig['bands'])):
        cats, labels = GetRelevantCatalogs(BalrogConfig, RunConfig, DerivedConfig, band=fixband)
        BalrogConfig = DoBandStuff(BalrogConfig, RunConfig, DerivedConfig['bands'][i], DerivedConfig['images'])
        NewWrite2DB(cats, labels, RunConfig, BalrogConfig, DerivedConfig)


def RunDoDES(RunConfig, BalrogConfig, DerivedConfig):
    BalrogConfig['noassoc'] = True
    BalrogConfig['nonosim'] = True
    BalrogConfig['ngal'] = 0
    BalrogConfig['image'] = DerivedConfig['images'][ DerivedConfig['iteration'][1] ]
    BalrogConfig['psf'] = DerivedConfig['psfs'][ DerivedConfig['iteration'][1] ]
    BalrogConfig = DoBandStuff(BalrogConfig, RunConfig, DerivedConfig['bands'][DerivedConfig['iteration'][1]], DerivedConfig['images'])
    BalrogConfig['outdir'] = os.path.join(DerivedConfig['outdir'], BalrogConfig['band'])
    if RunConfig['dualdetection']!=None:
        BalrogConfig['detimage'] = DerivedConfig['images'][0]
        BalrogConfig['detpsf'] = DerivedConfig['psfs'][0]

    cmd = Dict2Cmd(BalrogConfig, RunConfig['balrog'])
    subprocess.call(cmd)

    cats, labels = GetRelevantCatalogs(BalrogConfig, RunConfig, DerivedConfig)
    NewWrite2DB(cats, labels, RunConfig, BalrogConfig, DerivedConfig)


def GetBalroggedDetImage(DerivedConfig):
    band = DerivedConfig['bands'][0]
    inimage = os.path.basename(DerivedConfig['images'][0])
    outimage = inimage.replace('.fits', '.sim.fits')
    file = os.path.join(DerivedConfig['outdir'], band, 'balrog_image', outimage)
    return file


def RunNormal(RunConfig, BalrogConfig, DerivedConfig):
    coordfile = WriteCoords(DerivedConfig['pos'], DerivedConfig['outdir'])
    detimage = GetBalroggedDetImage(DerivedConfig)
    detpsf = DerivedConfig['psfs'][0]

    #for i in range(len(DerivedConfig['bands'])):
    for i in range(len(DerivedConfig['bands'][0:1])):
        BalrogConfig['poscat'] = coordfile
        BalrogConfig['image'] = DerivedConfig['images'][i]
        BalrogConfig['psf'] = DerivedConfig['psfs'][i]
        BalrogConfig = DoBandStuff(BalrogConfig, RunConfig, DerivedConfig['bands'][i], DerivedConfig['images'], doprint=False)
        BalrogConfig['outdir'] = os.path.join(DerivedConfig['outdir'], BalrogConfig['band'])

        if (RunConfig['dualdetection']!=None) and (i > 0):
            BalrogConfig['detimage'] = detimage
            BalrogConfig['detpsf'] = detpsf

        cmd = Dict2Cmd(BalrogConfig, RunConfig['balrog'])
        subprocess.call(cmd)

        cats, labels = GetRelevantCatalogs(BalrogConfig, RunConfig, DerivedConfig)
        NewWrite2DB(cats, labels, RunConfig, BalrogConfig, DerivedConfig)



def run_balrog(args):
    RunConfig, BalrogConfig, DerivedConfig = args
    it = EnsureInt(DerivedConfig)

    if it==-2:
        RunOnlyCreate(RunConfig, BalrogConfig, DerivedConfig)
    elif it==-1:
        RunDoDES(RunConfig, BalrogConfig, DerivedConfig)
    else:
        RunNormal(RunConfig, BalrogConfig, DerivedConfig)

    if RunConfig['intermediate-clean']:
        subprocess.call( ['rm', '-r', BalrogConfig['outdir']] )

    #print 'done'


def NewRunBalrog(RunConfig, BalrogConfig, DerivedConfig):
    workingdir = os.path.join(RunConfig['outdir'], RunConfig['label'], DerivedConfig['tile'] )
    indir = os.path.join(workingdir, 'input')
    Mkdir(indir)

    if RunConfig['fixwrapseed'] != None:
        DerivedConfig['seedoffset'] = RunConfig['fixwrapseed']
    else:
        DerivedConfig['seedoffset'] = np.random.randint(10000)


    DerivedConfig['images'], DerivedConfig['psfs'] = DownloadImages(indir, DerivedConfig['images'], DerivedConfig['psfs'], skip=False)
    BalrogConfig['tile'] = DerivedConfig['tile']
    DerivedConfig['bands'] = PrependDet(RunConfig)

    args = []
    inc = 0
    for it in DerivedConfig['iterations']:
        outdir = os.path.join(workingdir, 'output', '%i'%it)
        Mkdir(outdir)
        
        DConfig = copy.copy(DerivedConfig)
        DConfig['iteration'] = it
        DConfig['outdir'] = outdir

        BConfig = copy.copy(BalrogConfig)
        
        BConfig['indexstart'] = DConfig['indexstart']
        if it > 0:
            BConfig['indexstart'] += it*BConfig['ngal']
        BConfig['seed'] = BConfig['indexstart'] + DConfig['seedoffset']

        if it==-2:
            DConfig['pos'] = None
            arg = [RunConfig, BConfig, DConfig]
            args.append(arg)

        elif it==-1:
            DConfig['pos'] = None
            for i in range(len(DerivedConfig['bands'])):
                DDConfig = copy.copy(DConfig)
                DDConfig['iteration'] = (it,i)
                arg = [RunConfig, BConfig, DDConfig]
                args.append(arg)
        else:
            DConfig['pos'] = DerivedConfig['pos'][inc]
            arg = [RunConfig, BConfig, DConfig]
            args.append(arg)

        inc += 1
   
    nthreads = cpu_count()
    pool = Pool(nthreads)
    pool.map(run_balrog, args, chunksize=1)
    '''
    for arg in args:
        run_balrog(args[0])
    '''

    if RunConfig['tile-clean']:
        subprocess.call( ['rm', '-r', workingdir] )
