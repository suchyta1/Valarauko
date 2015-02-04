#!/usr/bin/env python

import cx_Oracle
import copy
import time
import datetime
import StringIO
import socket
import logging

import sys
#import pickle
#sys.modules['cPickle'] = pickle

import os
import re
import subprocess
from multiprocessing import Pool, cpu_count, Lock
import argparse
import logging
import threading
#import pyfits
import astropy.io.fits as pyfits
import desdb
import numpy as np
import numpy.lib.recfunctions as recfunctions
from mpi4py import MPI




def Remove(file):
    if os.path.lexists(file):
        os.remove(file)

def Mkdir(dir):
    if not os.path.lexists(dir):
        os.makedirs(dir)


# Download and uncompress images
def DownloadImages(indir, images, psfs, RunConfig, skip=False):
    useimages = []

    for file in images:
        infile = os.path.join(indir, os.path.basename(file))
        if not skip:
            Remove(infile)
            subprocess.call( ['wget', '-q', '--no-check-certificate', file, '-O', infile] )
        ufile = infile.replace('.fits.fz', '.fits')
        if not skip:
            Remove(ufile) 
            subprocess.call([RunConfig['funpack'], '-O', ufile, infile])
        useimages.append(ufile)

    usepsfs = []
    for psf in psfs:
        pfile = os.path.join(indir, os.path.basename(psf))
        if not skip:
            Remove(pfile)
            subprocess.call( ['wget', '-q', '--no-check-certificate', psf, '-O', pfile] )
        usepsfs.append(pfile)

    return [useimages, usepsfs]


# Convert Balrog dictionary to command line arguments
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


def DetBands(RunConfig):
    index = np.array(RunConfig['dualdetection'])
    bands =  np.array(RunConfig['bands'])
    detbands = ','.join(bands[index] )
    return detbands

def DetZps(RunConfig, DerivedConfig, ext=0, zpkey='SEXMGZPT'):
    zps = []
    ws = []
    fs = []
    inc = 0
    for i in RunConfig['dualdetection']:
        num = i + 1
        header = pyfits.open(DerivedConfig['images'][num])[ext].header
        zp = header[zpkey]
        zps.append(zp)

        file = DerivedConfig['images'][num]
        weight = pyfits.open(file)[ext+1].data
        w = np.mean(weight)
        ws.append(w)
        fs.append(file)

    return [zps, ws, fs]

def GetZeropoint(RunConfig, DerivedConfig,BalrogConfig, ext=0, zpkey='SEXMGZPT'):
    if BalrogConfig['band']=='det':
        #zps, ws, fs = DetZps(RunConfig, DerivedConfig, ext=ext, zpkey=zpkey)
        #return np.amin(zps)
        return 30.0
    else:
        header = pyfits.open(BalrogConfig['image'])[ext].header
        return header[zpkey]

def GetDetZps(RunConfig, DerivedConfig, ext=0, zpkey='SEXMGZPT'):
    zps, ws, fs = DetZps(RunConfig, DerivedConfig, ext=ext, zpkey=zpkey)
    for i in range(len(zps)):
        zps[i] = str(zps[i])
        ws[i] = str(ws[i])
    return [','.join(zps), ','.join(ws), ','.join(fs)]


"""
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

    if BalrogConfig['band']=='det':
        #BalrogConfig['zeropoint'] = np.average(zps)
        BalrogConfig['zeropoint'] = np.amin(zps)
    else:
        header = pyfits.open(BalrogConfig['image'])[ext].header
        BalrogConfig['zeropoint'] = header[zpkey]

    for i in range(len(zps)):
        zps[i] = str(zps[i])

    BalrogConfig['detzeropoints'] = ','.join(zps)
    if doprint:
        print BalrogConfig['detzeropoints'], BalrogConfig['detbands'], BalrogConfig['zeropoint'], BalrogConfig['band']

    return BalrogConfig


def DoBandStuff(BalrogConfig, RunConfig, band, images, ext=0, zpkey='SEXMGZPT', doprint=False):
    #BalrogConfig['band'] = band
    '''
    if band=='det':
        BalrogConfig = GetDetStuff(BalrogConfig, RunConfig, images, ext=ext, zpkey=zpkey, doprint=doprint)
    '''
    '''
    if RunConfig['dualdetection']!=None:
        BalrogConfig = GetDetStuff(BalrogConfig, RunConfig, images, ext=ext, zpkey=zpkey, doprint=doprint)
    '''

    return BalrogConfig


def GetSeed(BalrogConfig, DerivedConfig):

    BalrogConfig['seed'] = BalrogConfig['indexstart'] + DerivedConfig['seedoffset']
    return BalrogConfig
"""


# Figure out which catalogs get writting to which DB tables
def GetRelevantCatalogs(BalrogConfig, RunConfig, DerivedConfig, band=None, create=False, appendsim=False):
    it = EnsureInt(DerivedConfig)
    if band==None:
        band = BalrogConfig['band']

    extra = ''
    if appendsim:
        extra = 'sim.'

    out_truth = os.path.join(BalrogConfig['outdir'], 'balrog_cat', '%s_%s.%struthcat.sim.fits'%(BalrogConfig['tile'],band,extra))
    out_nosim = os.path.join(BalrogConfig['outdir'], 'balrog_cat', '%s_%s.%smeasuredcat.nosim.fits'%(BalrogConfig['tile'],band,extra))
    out_sim = os.path.join(BalrogConfig['outdir'], 'balrog_cat', '%s_%s.%smeasuredcat.sim.fits'%(BalrogConfig['tile'],band,extra))

    relevant = {'truth': True, 'nosim': True, 'sim': True}

    if 'nonosim' in BalrogConfig.keys():
        if BalrogConfig['nonosim']:
            relevant['nosim'] = False
    if it==-1:
        relevant['truth'] = False
        relevant['nosim'] = False

    if 'imageonly' in BalrogConfig.keys():
        if BalrogConfig['imageonly']:
            relevant['nosim'] = False
            relevant['sim'] = False
    if 'nodraw' in BalrogConfig.keys():
        if BalrogConfig['nodraw']:
            relevant['truth'] = False


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

    if create:
        files = [out_truth, out_nosim, out_sim, out_sim]
        labels = ['truth', 'nosim', 'sim', 'des']


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


# Remove VECASSOC in output catalog in favor of balrog_index
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


# Change some protected Oracle keywords and add the tilename to all tables
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


# How to connect sqlldr to the DB
def get_sqlldr_connection_info(db_specs):
    cur = desdb.connect()
    return '%s/%s@"(DESCRIPTION=(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%s))(CONNECT_DATA=(SERVER=%s)(SERVICE_NAME=%s)))"' %(cur.username,cur.password,db_specs['protocol'],db_specs['db_host'],db_specs['port'],db_specs['server'],db_specs['service_name'])


# Write Balrog catalogs to DB
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


#How to connect to DB in cx_Oracle
def get_cx_oracle_cursor(db_specs):
    c = desdb.connect()
    connection = cx_Oracle.connect( "%s/%s@(DESCRIPTION=(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%s))(CONNECT_DATA=(SERVER=%s)(SERVICE_NAME=%s)))" %(c.username,c.password,db_specs['protocol'],db_specs['db_host'],db_specs['port'],db_specs['server'],db_specs['service_name']) )
    cur = connection.cursor()
    return cur, connection

 
# Convert numpy array to something can be written to the DB by cx_Oracle
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


# Convert numpy array to something can be written to the DB by cx_Oracle
def GetOracleStructure(arr, tablename, noarr=False, create=False):
    a = arr.view(np.ndarray)
    cs, alldefs = desdb.get_tabledef(a.dtype.descr, tablename)

    if create:
        return cs
    else:
        istr, newarr = MakeNewArray(alldefs, arr, tablename, noarr=noarr)
        return istr, newarr


# Write coordinates of simulated galaxies to a file so Balrog can read them in
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

    BalrogConfig['band'] = DerivedConfig['bands'][0]
    if RunConfig['dualdetection']==None:
        BalrogConfig['band'] = RunConfig['bands'][0]

    BalrogConfig['zeropoint'] = GetZeropoint(RunConfig, DerivedConfig, BalrogConfig)
    BalrogConfig['nonosim'] = False
    BalrogConfig['outdir'] = os.path.join(DerivedConfig['outdir'], BalrogConfig['band'])
    cmd = Dict2Cmd(BalrogConfig, RunConfig['balrog'])
    subprocess.call(cmd)

    fixband = BalrogConfig['band']
    for i in range(len(DerivedConfig['bands'])):
        cats, labels = GetRelevantCatalogs(BalrogConfig, RunConfig, DerivedConfig, band=fixband, create=True)
        BalrogConfig['band'] = DerivedConfig['bands'][i]
        NewWrite2DB(cats, labels, RunConfig, BalrogConfig, DerivedConfig)


def RunDoDES(RunConfig, BalrogConfig, DerivedConfig):
    BalrogConfig['noassoc'] = True
    BalrogConfig['nonosim'] = True
    BalrogConfig['ngal'] = 0
    BalrogConfig['image'] = DerivedConfig['images'][ DerivedConfig['iteration'][1] ]
    BalrogConfig['psf'] = DerivedConfig['psfs'][ DerivedConfig['iteration'][1] ]

    BalrogConfig['band'] = DerivedConfig['bands'][DerivedConfig['iteration'][1]]
    BalrogConfig['zeropoint'] = GetZeropoint(RunConfig, DerivedConfig, BalrogConfig)
    '''
    if RunConfig['dualdetection']!=None:
        BalrogConfig['detbands'] = DetBands(RunConfig)
        BalrogConfig['detzeropoints'], BalrogConfig['detweights'], BalrogConfig['detfiles'] = GetDetZps(RunConfig, DerivedConfig)
    '''

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


def SwarpConfig(imgs, RunConfig, DerivedConfig, BalrogConfig, iext=0, wext=1):
    config = {'RESAMPLE': 'N',
              'COMBINE': 'Y',
              'COMBINE_TYPE': 'CHI-MEAN',
              'SUBTRACT_BACK': 'N',
              'DELETE_TMPFILES': 'Y',
              'WEIGHT_TYPE': 'MAP_WEIGHT',
              'PIXELSCALE_TYPE': 'MANUAL',
              'PIXEL_SCALE': str(0.270),
              'CENTER_TYPE': 'MANUAL',
              'HEADER_ONLY': 'N',
              'WRITE_XML': 'N'}

    header = pyfits.open(imgs[0])[iext].header
    xsize = header['NAXIS1']
    ysize = header['NAXIS2']
    config['IMAGE_SIZE'] = '%i,%i' %(xsize,ysize)
    xc = header['CRVAL1']
    yc = header['CRVAL2']
    config['CENTER'] = '%f,%f' %(xc,yc)

    ims = []
    ws = []
    for i in range(len(imgs)):
        ims.append( '%s[%i]' %(imgs[i],iext) )
        ws.append( '%s[%i]' %(imgs[i],wext) )
    ims = ','.join(ims)
    ws = ','.join(ws)

    dir = os.path.join(DerivedConfig['outdir'], 'det')
    Mkdir(dir)
    imout = os.path.join(dir, '%s_det.fits'%(BalrogConfig['tile']))
    wout = imout.replace('.fits', '_weight.fits')
    config['IMAGEOUT_NAME'] = imout
    config['WEIGHTOUT_NAME'] = wout
    
    call = [RunConfig['swarp'], ims, '-c', RunConfig['swarp-config'], '-WEIGHT_IMAGE', ws]
    for key in config:
        call.append( '-%s'%(key) )
        call.append( config[key] )
    return call, imout, wout



# This will need to be changed to coadd the detection image
def RunNormal(RunConfig, BalrogConfig, DerivedConfig):
    
    #global runlog

    coordfile = WriteCoords(DerivedConfig['pos'], DerivedConfig['outdir'])
    BalrogConfig['poscat'] = coordfile
    if RunConfig['dualdetection']!=None:
        #runlog.info('%s %s %s' %('a', DerivedConfig['iteration'], socket.gethostname()))
        BConfig = copy.copy(BalrogConfig)
        BConfig['imageonly'] = True
        detbands = DetBands(RunConfig)
        dbands = detbands.split(',')
        cimages = {}
        cimgs = []
        for i, band in zip(RunConfig['dualdetection'], dbands):
            #runlog.info('%s %s %s' %('b', DerivedConfig['iteration'], socket.gethostname()))
            img = DerivedConfig['images'][i+1]
            BConfig['image'] = img
            BConfig['psf'] = DerivedConfig['psfs'][i+1]
            BConfig['band'] = band
            BConfig['zeropoint'] = GetZeropoint(RunConfig, DerivedConfig, BConfig)
            BConfig['outdir'] = os.path.join(DerivedConfig['outdir'], BConfig['band'])
            outimage = os.path.basename(img).replace('.fits', '.sim.fits')
            outfile = os.path.join(BConfig['outdir'], 'balrog_image', outimage)
            cimages[band] = outfile
            cimgs.append(outfile)
            cmd = Dict2Cmd(BConfig, RunConfig['balrog'])
            #runlog.info('%s %s %s %s' %('c', BConfig['band'], DerivedConfig['iteration'], socket.gethostname()))
            subprocess.call(cmd)
            #runlog.info('%s %s %s %s' %('d', BConfig['band'], DerivedConfig['iteration'], socket.gethostname()))

            cats, labels = GetRelevantCatalogs(BConfig, RunConfig, DerivedConfig)
            NewWrite2DB(cats, labels, RunConfig, BConfig, DerivedConfig)
            #runlog.info('%s %s %s %s' %('e', BConfig['band'], DerivedConfig['iteration'], socket.gethostname()))

        cmd, detimage, detwimage = SwarpConfig(cimgs, RunConfig, DerivedConfig, BConfig)

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        swarplog = logging.getLogger('swarp')
        swarplogfile = detimage.replace('.fits', '.log')
        fh = logging.FileHandler(swarplogfile, mode='w')
        fh.setLevel(logging.INFO)
        swarplog.addHandler(fh)
        
        #runlog.info('%s %s %s' %('f', DerivedConfig['iteration'], socket.gethostname()))
        with lock:
            swarplog.info('# Exact command call')
            swarplog.info(' '.join(cmd))
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            swarplog.info(stdout)
            swarplog.info(stderr)
            swarplog.info('\n')
        #runlog.info('%s %s %s' %('g', DerivedConfig['iteration'], socket.gethostname()))

    detpsf = DerivedConfig['psfs'][0]
    for i in range(len(DerivedConfig['bands'])):
        appendsim = False
        BConfig = copy.copy(BalrogConfig)
        BConfig['psf'] = DerivedConfig['psfs'][i]
        BConfig['band'] = DerivedConfig['bands'][i]
        BConfig['outdir'] = os.path.join(DerivedConfig['outdir'], BConfig['band'])
        BConfig['image'] = DerivedConfig['images'][i]
        BConfig['zeropoint'] = GetZeropoint(RunConfig, DerivedConfig, BConfig)

        if RunConfig['dualdetection']!=None:
            BConfig['detimage'] = detimage
            BConfig['detweight'] = detwimage
            BConfig['detpsf'] = detpsf
            
            band = BConfig['band']
            if band=='det':
                BConfig['nodraw'] = True
                BConfig['image'] = detimage
                BConfig['weight'] = detwimage

            elif BConfig['band'] in detbands.split(','):
                BConfig['nodraw'] = True
                BConfig['image'] = cimages[band]
                appendsim = True


        #runlog.info('%s %s %s %s' %('h', BConfig['band'], DerivedConfig['iteration'], socket.gethostname()))
        cmd = Dict2Cmd(BConfig, RunConfig['balrog'])
        subprocess.call(cmd)
        #runlog.info('%s %s %s %s' %('j', BConfig['band'], DerivedConfig['iteration'], socket.gethostname()))

        cats, labels = GetRelevantCatalogs(BConfig, RunConfig, DerivedConfig, appendsim=appendsim)
        NewWrite2DB(cats, labels, RunConfig, BConfig, DerivedConfig)
        #runlog.info('%s %s %s %s' %('k', BConfig['band'], DerivedConfig['iteration'], socket.gethostname()))



def run_balrog(args):
    RunConfig, BalrogConfig, DerivedConfig = args
    it = EnsureInt(DerivedConfig)

    if it==-2:
        # Minimal Balrog run to create DB tables
        RunOnlyCreate(RunConfig, BalrogConfig, DerivedConfig)
    elif it==-1:
        # No simulated galaxies
        RunDoDES(RunConfig, BalrogConfig, DerivedConfig)
    else:
        # Actual Balrog realization
        RunNormal(RunConfig, BalrogConfig, DerivedConfig)

    if RunConfig['intermediate-clean']:
        if it < 0:
                subprocess.call( ['rm', '-r', BalrogConfig['outdir']] )
        else:
            for band in DerivedConfig['bands']:
                dir = os.path.join(DerivedConfig['outdir'], band)
                subprocess.call( ['rm', '-r', dir] )



lock = Lock()
#runlog = None




def MPIRunBalrog(RunConfig, BalrogConfig, DerivedConfig):
    '''
    host = socket.gethostname()
    logfile = '%s-%s' %(RunConfig['label'], host)
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)
    global runlog
    runlog = logging.getLogger('run')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(logfile, mode='w')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    runlog.addHandler(fh)
    '''

    Mkdir(DerivedConfig['indir'])
    DerivedConfig['images'], DerivedConfig['psfs'] = DownloadImages(DerivedConfig['indir'], DerivedConfig['images'], DerivedConfig['psfs'], RunConfig, skip=DerivedConfig['initialized'])
    Mkdir(DerivedConfig['outdir'])

    if (DerivedConfig['iteration']!=-2) and (DerivedConfig['initialized']==False):
        host = socket.gethostname()
        rank = MPI.COMM_WORLD.Get_rank()
        send = -3
        MPI.COMM_WORLD.sendrecv([rank,host,send], dest=0, source=0)

    run_balrog( [RunConfig, BalrogConfig, DerivedConfig] )