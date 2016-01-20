#!/usr/bin/env python

import cx_Oracle
import copy
import socket
import logging
import datetime
import time
import resource
import shutil

import sys
import os
import re
import subprocess

import astropy.io.fits as pyfits
import desdb
import numpy as np
import numpy.lib.recfunctions as recfunctions
from mpi4py import MPI
import AllMpi
import balrog
import traceback
import fitsio


def Remove(file):
    if os.path.lexists(file):
        os.remove(file)

def Mkdir(dir):
    if not os.path.lexists(dir):
        os.makedirs(dir)

def BalrogSystemCall(cmd, DerivedConfig, func=True):
    if func:
        msg = 'Doing Balrog as function: \n%s\n'%(' '.join(cmd[2:]))
        balrog.SysInfoPrint(DerivedConfig['setup'], msg, level='info')
        #balrog.BalrogFunction(args=cmd[3:], systemredirect=DerivedConfig['itlog'], excredirect=DerivedConfig['itlog'])
        balrog.BalrogFunction(args=cmd[3:], syslog=DerivedConfig['itlog'])
    else:
        balrog.SystemCall(args, setup=DerivedConfig['setup'])


def ImageDownload(indir, file, DerivedConfig, RunConfig, skip):
    infile = os.path.join(indir, os.path.basename(file))
    if not skip:
        Remove(infile)
        oscmd = ['wget', '--quiet', '--no-check-certificate', file, '-O', infile]
        balrog.SystemCall(oscmd, setup=DerivedConfig['setup'])
        if not os.path.exists(infile):
            return ImageDownload(indir, file, DerivedConfig, RunConfig, skip)
    ufile = infile.replace('.fits.fz', '.fits')
    if not skip:
        Remove(ufile) 
        oscmd = [RunConfig['funpack'], '-O', ufile, infile]
        balrog.SystemCall(oscmd, setup=DerivedConfig['setup'])
        if not os.path.exists(ufile):
            return ImageDownload(indir, file, DerivedConfig, RunConfig, skip)
        try:
            f = fitsio.FITS(ufile)
        except:
            return ImageDownload(indir, file, DerivedConfig, RunConfig, skip)
    return ufile


def PSFDownload(indir, psf, DerivedConfig, RunConfig, skip):
    pfile = os.path.join(indir, os.path.basename(psf))
    if not skip:
        Remove(pfile)
        oscmd = ['wget', '--quiet', '--no-check-certificate', psf, '-O', pfile]
        balrog.SystemCall(oscmd, setup=DerivedConfig['setup'])
        if not os.path.exists(pfile):
            return PSFDownload(indir, psf, DerivedConfig, RunConfig, skip)
        try:
            f = fitsio.FITS(pfile)
        except:
            return PSFDownload(indir, psf, DerivedConfig, RunConfig, skip)
    return pfile


# Download and uncompress images
def DownloadImages(indir, images, psfs, RunConfig, DerivedConfig, skip=False):
    useimages = []

    for file in images:
        ufile = ImageDownload(indir, file, DerivedConfig, RunConfig, skip)
        useimages.append(ufile)

    usepsfs = []
    for psf in psfs:
        pfile = PSFDownload(indir, psf, DerivedConfig, RunConfig, skip)
        usepsfs.append(pfile)

    return [useimages, usepsfs]


# Convert Balrog dictionary to command line arguments
def Dict2Cmd(d, cmd):

    #l = [cmd]
    l = ['python', '-s', cmd]

    for key in d.keys():
        if type(d[key])==bool:
            if d[key]:
                l.append('--%s' %key)
        else:
            l.append('--%s' %key)
            l.append(str(d[key]))

    #l = ' '.join(l)
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


def GetRelevantCatsBands2(files, allband, bands, labels, missingfix='i', allfix=None):
    valids = []
    newfiles = copy.copy(files)
    for i in range(len(files)):
        readband = allband
        valid = True
        truth = False
        if labels[i]=='truth':
            truth = True

        if truth and (allband=='det'):
            readband = missingfix
            valid = False

        if allband not in bands: 
            readband = missingfix
            valid = False


        if allfix is not None:
            readband = allfix

        valids.append(valid)
        newfiles[i] = files[i].replace('<band>', readband)

    return newfiles, valids
        


def GetRelevantCatsBase(it, BalrogConfig, RunConfig, DerivedConfig, sim2nosim=False, extra='', create=False):
    outdir = BalrogConfig['outdir']
    while (outdir[-1]=='/'):
        outdir = outdir[:-1]

    outdir = os.path.join(os.path.dirname(outdir), '<band>', 'balrog_cat')
    out_nosim = os.path.join(outdir, '%s_<band>.%smeasuredcat.nosim.fits'%(BalrogConfig['tile'],extra))
    out_sim = os.path.join(outdir, '%s_<band>.%smeasuredcat.sim.fits'%(BalrogConfig['tile'],extra))

    extra=""
    out_truth = os.path.join(outdir, '%s_<band>.%struthcat.sim.fits'%(BalrogConfig['tile'],extra))

    #/data/esuchyta/BalrogScratch/y1a1_test/DES0451-3832/output/0/det/balrog_cat/DES0451-3832_det.measuredcat.sim.fits

    #out_truth = os.path.join(BalrogConfig['outdir'], 'balrog_cat', '%s_<band>.%struthcat.sim.fits'%(BalrogConfig['tile'],extra))
    #out_nosim = os.path.join(BalrogConfig['outdir'], 'balrog_cat', '%s_<band>.%smeasuredcat.nosim.fits'%(BalrogConfig['tile'],extra))
    #out_sim = os.path.join(BalrogConfig['outdir'], 'balrog_cat', '%s_<band>.%smeasuredcat.sim.fits'%(BalrogConfig['tile'],extra))

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
        if sim2nosim:
            labels.append('nosim')
        else:
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


def GetRelevantCats2(BalrogConfig, RunConfig, DerivedConfig, allfix=None, missingfix='i', create=False, appendsim=False, sim2nosim=False):
    it = EnsureInt(DerivedConfig)
    bands = DerivedConfig['imbands']
    allbands = AllMpi.GetAllBands()


    fs = []
    vs = []
    for i in range(len(allbands)):

        extra = ''
        if (allbands[i] in DetBands(RunConfig)) and (it>=0) and (appendsim):
            extra = 'sim.'
        basefiles, labels = GetRelevantCatsBase(it, BalrogConfig, RunConfig, DerivedConfig, sim2nosim=sim2nosim, extra=extra, create=create)

        files, valid = GetRelevantCatsBands2(basefiles, allbands[i], bands, labels, missingfix=missingfix, allfix=allfix)
        fs.append(files)
        vs.append(valid)
    
    return fs, labels, vs



# Figure out which catalogs get writting to which DB tables
def GetRelevantCatalogs(BalrogConfig, RunConfig, DerivedConfig, band=None, create=False, appendsim=False, sim2nosim=False):
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
        if sim2nosim:
            labels.append('nosim')
        else:
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
def NewMakeOracleFriendly(file, ext, BalrogConfig, DerivedConfig, label, RunConfig, missingfix='i', create=False):
    if (file.find('truth')!=-1) and (file.find('det')!=-1) and not create:
        file = file.replace('det', missingfix)

    hdu = pyfits.open(file)[ext]
    header = hdu.header
    ndata = np.array(hdu.data)

    descr = ndata.dtype.descr

    if label in ['nosim', 'sim', 'des']:
        ndata = Number2NumberSex(ndata)
        if ((label!='des') or (DerivedConfig['iteration']==-2)) and (('noassoc' not in BalrogConfig.keys()) or (BalrogConfig['noassoc']==False)):
            ndata = VecAssoc2BalrogIndex(header, ndata, label)
        
    t = np.array( [BalrogConfig['tile']]*len(ndata) )
    ndata = recfunctions.append_fields(ndata, 'tilename', t, '|S12', usemask=False)

    num = np.array( [RunConfig['runnum']]*len(ndata) )
    ndata = recfunctions.append_fields(ndata, 'runnum', num, usemask=False)

    return ndata


# How to connect sqlldr to the DB
def get_sqlldr_connection_info(db_specs):
    cur = desdb.connect()
    #return '%s/%s@"(DESCRIPTION=(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%s))(CONNECT_DATA=(SERVER=%s)(SERVICE_NAME=%s)))"' %(cur.username,cur.password,db_specs['protocol'],db_specs['db_host'],db_specs['port'],db_specs['server'],db_specs['service_name'])
    return '%s/%s@"\(DESCRIPTION=\(ADDRESS=\(PROTOCOL=%s\)\(HOST=%s\)\(PORT=%s\)\)\(CONNECT_DATA=\(SERVER=%s\)\(SERVICE_NAME=%s\)\)\)"' %(cur.username,cur.password,db_specs['protocol'],db_specs['db_host'],db_specs['port'],db_specs['server'],db_specs['service_name'])



def AllDefs(arr, tablename):
    a = arr.view(np.ndarray)
    create_cmd, alldefs = desdb.desdb.get_tabledef(a.dtype.descr, tablename)
    return create_cmd, alldefs

def desdm_names(file):
    #file = '%s_objects-columns.fits' %(release)
    cnames = pyfits.open(file)[1].data['column_name']
    names = []
    for c in cnames:
        names.append(c.strip().lower())
    return names


def UpdateCreates(arr, tablename, creates, names, j, i, singles, allbands, dbfile, required='i', truth=False, truth_cols=[]):
    #create_cmd = GetOracleStructure(arr, tablename, noarr=noarr, create=True)
    create_cmd, alldefs = AllDefs(arr, tablename)
    create_cmd = create_cmd.replace('not null', 'null')
    cc = create_cmd.split('\n')

    desdm_cols = desdm_names(dbfile)
    obs_one = ObsOne()

    cols = cc[1:-2]
    if j==0:
        creates.append( [cc[0]] )
        names.append(tablename)

    for k in range(len(cols)):
        c = cols[k].strip().split()
    
        cw = '%s_%s' %(c[0],allbands[j])
        if not truth:
            if allbands[j]=='det':
                cww = cw.lower().replace('_det', '_%s'%(required))
            else:
                cww = cw

            if (cww.lower() not in desdm_cols) and (c[0].lower() not in obs_one):
                continue

        if c[0].lower() in singles:
            if allbands[j]!=required:
                continue
            c[-1] = c[-1].replace('null', 'not null')
        else:
            #c[0] = '%s_%s' %(c[0],allbands[j])
            c[0] = cw

        if c[-1][-1]!=',':
            c[-1] = c[-1] + ','

        creates[i].append( ' '.join(c) )
    
    if j==(len(allbands)-1):
        creates[i] = '\n'.join(creates[i])[:-1]
        creates[i] = '%s %s' %(creates[i],cc[-2])

    return creates, names


def NewWrite2DB2(bcats, labels, valids, RunConfig, BalrogConfig, DerivedConfig, required='i', missingfix='i'):
    singles = OneOnly()
    it = EnsureInt(DerivedConfig)
    create = False
    if it==-2:
        create = True

    cur = desdb.connect()
    cxcur, con = get_cx_oracle_cursor(DerivedConfig['db'])
    allbands = AllMpi.GetAllBands()

    creates = []
    names = []
    dobj = []

    for j in range(len(allbands)):
        cats = bcats[j]

        for i in range(len(cats)):
            ext = 1
            if labels[i]!='truth' and BalrogConfig['catfitstype']=='ldac':
                ext = 2

            cat = cats[i]
            tablename = '%s.balrog_%s_%s' %(cur.username, RunConfig['label'], labels[i])
            arr = NewMakeOracleFriendly(cats[i], ext, BalrogConfig, DerivedConfig, labels[i], RunConfig, missingfix=missingfix, create=create)

            noarr = False
            t = False
            if labels[i]=='truth':
                noarr = True
                t = True

            if create:
                creates, names = UpdateCreates(arr, tablename, creates, names, j, i, singles, allbands, RunConfig['db-columns'], required=required, truth=t)

            else:
                dobj = UpdateInserts(arr, tablename, noarr, j, i, allbands, dobj, singles, valids, RunConfig['db-columns'], required=required, truth=t)

    if create:
        for i in range(len(creates)):
            cur.quick(creates[i])
            cur.quick("GRANT SELECT ON %s TO DES_READER" %names[i])

    else:
        for i in range(len(dobj)):
            nums = []
            for j in range(len(dobj[i]['num'])):
                n = ':%i' %(dobj[i]['num'][j])
                nums.append(n)

            numstr = ', '.join(nums)
            namestr = ', '.join(dobj[i]['name'])
            newarr = zip(*dobj[i]['list'])

            tablename = '%s.balrog_%s_%s' %(cur.username, RunConfig['label'], labels[i])
            istr = "insert into %s (%s) values (%s)" %(tablename, namestr, numstr)

            cxcur.prepare(istr)
            #print bcats, valids

            '''
            try:
                cxcur.executemany(None, newarr)
                print 'good arr', newarr
            except:
                print 'bad arr', newarr
            '''

            cxcur.executemany(None, newarr)
            con.commit()

    cxcur.close()


def UpdateInserts(arr, tablename, noarr, j, i, allbands, dobj, singles, valids, dbfile, required='i', truth=False):
    create_cmd, alldefs = AllDefs(arr, tablename)
    desdm_cols = desdm_names(dbfile)
    obs_one = ObsOne()

    if j==0:
        dobj.append( {} )
        dobj[i]['list'] = []
        dobj[i]['num'] = []
        dobj[i]['name'] = []

    for k in range(len(alldefs)):

        name = alldefs[k][0]
        if noarr:
            isarr = None
        else:
            isarr = re.search(r'_\d+$', name)


        cw = '%s_%s' %(name,allbands[j])
        if not truth:
            if allbands[j]=='det':
                cww = cw.lower().replace('_det', '_%s'%(required))
            else:
                cww = cw

            if (cww.lower() not in desdm_cols) and (name.lower() not in obs_one):
                continue

        if name.lower() in singles:
            if allbands[j]!=required:
                continue
            newname = name
        else:
            #c[0] = '%s_%s' %(c[0],allbands[j])
            newname = cw


        '''
        if not valids[j][i]:
            arr[name][:] = None
        '''


        if isarr==None:
            litem = (arr[name]).tolist()
            nn = arr[name]
        else:
            n = name[ : (isarr.span()[0]) ]
            m = int( isarr.group(0)[1:] ) - 1
            litem = (arr[n][:,m]).tolist()
            nn = arr[n][:,m]

        if not valids[j][i]:
            litem = [None]*len(litem)
        if name not in ['tilename']:
            cut = np.isnan(nn)
            where = np.arange(len(cut))[cut]
            for w in where:
                litem[w] = None


        dobj[i]['list'].append(litem)
        dobj[i]['name'].append(newname)
        if (len(dobj[i]['num']) > 0):
            dobj[i]['num'].append( dobj[i]['num'][-1] + 1 )
        else:
            dobj[i]['num'].append(1)

    return dobj


def ObsOne():
    return ['balrog_index', 'tilename', 'number_sex', 'runnum']

def OneOnly():
    return ['balrog_index', 'tilename',
            'x','y','g1','g2','magnification','halflightradius_0','beta_0','sersicindex_0','axisratio_0',
            'ra','dec','id','mod', 'objtype','z','indexstart','seed',
            'number_sex', 'runnum']


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
    cs, alldefs = desdb.desdb.get_tabledef(a.dtype.descr, tablename)

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
    BalrogSystemCall(cmd, DerivedConfig, func=RunConfig['balrog_as_function'])
    #balrog.SystemCall(cmd, setup=DerivedConfig['setup'])

    fixband = BalrogConfig['band']
    cats, labels, valids = GetRelevantCats2(BalrogConfig, RunConfig, DerivedConfig, allfix='det', create=True, appendsim=False, sim2nosim=False)

    NewWrite2DB2(cats, labels, valids, RunConfig, BalrogConfig, DerivedConfig)


"""
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
    BalrogSystemCall(cmd, DerivedConfig, func=RunConfig['balrog_as_function'])
    #balrog.SystemCall(cmd, setup=DerivedConfig['setup'])

    cats, labels = GetRelevantCatalogs(BalrogConfig, RunConfig, DerivedConfig)
    NewWrite2DB(cats, labels, RunConfig, BalrogConfig, DerivedConfig)
"""


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
    #call = ' '.join(call)
    return call, imout, wout


def RunNormal2(RunConfig, BalrogConfig, DerivedConfig):
    
    coordfile = WriteCoords(DerivedConfig['pos'], DerivedConfig['outdir'])
    BalrogConfig['poscat'] = coordfile
    if RunConfig['dualdetection']!=None:

        BConfig = copy.copy(BalrogConfig)
        BConfig['imageonly'] = False
        BConfig['nodraw'] = True
        BConfig['nonosim'] = True

        detpsf = DerivedConfig['psfs'][0]
        BConfig['detpsf'] = detpsf
        BConfig['detimage'] = DerivedConfig['images'][0]
        #BConfig['image'] = DerivedConfig['images'][0]


        #for k in range(len(DerivedConfig['images'])):
        for k in range(len(DerivedConfig['imbands'])):
            band = DerivedConfig['imbands'][k]
            BConfig['psf'] = DerivedConfig['psfs'][k]
            BConfig['image'] = DerivedConfig['images'][k]
            BConfig['band'] = band
            BConfig['outdir'] = os.path.join(DerivedConfig['outdir'], band)
            BConfig['zeropoint'] = GetZeropoint(RunConfig, DerivedConfig, BConfig)
            cmd = Dict2Cmd(BConfig, RunConfig['balrog'])
            BalrogSystemCall(cmd, DerivedConfig, func=RunConfig['balrog_as_function'])

        cats, labels, valids = GetRelevantCats2(BConfig, RunConfig, DerivedConfig, allfix=None, missingfix='i', appendsim=False, sim2nosim=True, create=False)
        NewWrite2DB2(cats, labels, valids, RunConfig, BConfig, DerivedConfig)

        detpsf = DerivedConfig['psfs'][0]
        BConfig = copy.copy(BalrogConfig)
        BConfig['imageonly'] = True
        detbands = DetBands(RunConfig)
        dbands = detbands.split(',')
        cimages = {}
        cimgs = []
        for i, band in zip(RunConfig['dualdetection'], dbands):
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
            BalrogSystemCall(cmd, DerivedConfig, func=RunConfig['balrog_as_function'])

            #cats, labels, valid = GetRelevantCats2(BConfig, RunConfig, DerivedConfig)
            #NewWrite2DB2(cats, labels, valids, RunConfig, BConfig, DerivedConfig)

        #cats, labels, valids = GetRelevantCats2(BConfig, RunConfig, DerivedConfig)
        #NewWrite2DB2(cats, labels, valids, RunConfig, BConfig, DerivedConfig)

        cmd, detimage, detwimage = SwarpConfig(cimgs, RunConfig, DerivedConfig, BConfig)
        balrog.SystemCall(cmd, setup=DerivedConfig['setup'])



    for i in range(len(DerivedConfig['bands'])):
        appendsim = False
        BConfig = copy.copy(BalrogConfig)
        BConfig['psf'] = DerivedConfig['psfs'][i]
        BConfig['band'] = DerivedConfig['bands'][i]
        BConfig['outdir'] = os.path.join(DerivedConfig['outdir'], BConfig['band'])
        BConfig['image'] = DerivedConfig['images'][i]

        BConfig['zeropoint'] = GetZeropoint(RunConfig, DerivedConfig, BConfig)

        #BConfig['nonosim'] = False
        BConfig['nonosim'] = True

        if RunConfig['dualdetection']!=None:
            appendsim = True
            BConfig['detimage'] = detimage
            BConfig['detweight'] = detwimage
            BConfig['detpsf'] = detpsf

            #BConfig['nonosim'] = True
            
            band = BConfig['band']
            if band=='det':
                BConfig['nodraw'] = True
                BConfig['image'] = detimage
                BConfig['weight'] = detwimage

            elif BConfig['band'] in detbands.split(','):
                BConfig['nodraw'] = True
                BConfig['image'] = cimages[band]
                appendsim = True


        cmd = Dict2Cmd(BConfig, RunConfig['balrog'])
        BalrogSystemCall(cmd, DerivedConfig, func=RunConfig['balrog_as_function'])

    BConfig['nodraw'] = False
    cats, labels, valids = GetRelevantCats2(BConfig, RunConfig, DerivedConfig, appendsim=True)
    NewWrite2DB2(cats, labels, valids, RunConfig, BConfig, DerivedConfig)



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
        RunNormal2(RunConfig, BalrogConfig, DerivedConfig)

    if RunConfig['intermediate-clean']:
        if it < 0:
            shutil.rmtree(BalrogConfig['outdir'])
        else:
            for band in DerivedConfig['bands']:
                dir = os.path.join(DerivedConfig['outdir'], band)
                shutil.rmtree(dir)



def SetupLog(logfile, host, rank, tile, iteration):
    log = logging.getLogger('tile = %s, it = %s' %(tile, str(iteration) ))
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


def MPIRunBalrog(RunConfig, BalrogConfig, DerivedConfig):
    Mkdir(DerivedConfig['indir'])
    Mkdir(DerivedConfig['outdir'])

    host = socket.gethostname()
    rank = MPI.COMM_WORLD.Get_rank()

    if RunConfig['command']=='popen':
        DerivedConfig['itlog'] = SetupLog(DerivedConfig['itlogfile'], host, rank, BalrogConfig['tile'], DerivedConfig['iteration'])
    elif RunConfig['command']=='system':
        DerivedConfig['itlog'] = DerivedConfig['itlogfile']
    f = '%i-%s-%s-systemcall.tmp'%(MPI.COMM_WORLD.Get_rank(), RunConfig['label'], RunConfig['joblabel'])
    DerivedConfig['setup'] = balrog.SystemCallSetup(retry=RunConfig['retry'], redirect=DerivedConfig['itlog'], kind=RunConfig['command'], useshell=RunConfig['useshell'])


    DerivedConfig['images'], DerivedConfig['psfs'] = DownloadImages(DerivedConfig['indir'], DerivedConfig['images'], DerivedConfig['psfs'], RunConfig, DerivedConfig, skip=DerivedConfig['initialized'])


    if (DerivedConfig['iteration']!=-2) and (DerivedConfig['initialized']==False):
        send = -3
        MPI.COMM_WORLD.sendrecv([rank,host,send], dest=0, source=0)

    run_balrog( [RunConfig, BalrogConfig, DerivedConfig] )
