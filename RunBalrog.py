#!/usr/bin/env python

import cx_Oracle
import copy
import StringIO
import socket
import logging
import datetime
import resource

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


def Remove(file):
    if os.path.lexists(file):
        os.remove(file)

def Mkdir(dir):
    if not os.path.lexists(dir):
        os.makedirs(dir)

def SystemCall(cmd, redirect=None, kind='system'):

    if kind=='system':
        oscmd = subprocess.list2cmdline(cmd)
    else:
        oscmd = cmd

    if redirect==None:
        if kind=='system':
            os.system(oscmd)
        elif kind=='popen':
            p = subprocess.Popen(oscmd)
            p.wait()

    else:
        if kind=='system':
            host = socket.gethostname()
            rank = MPI.COMM_WORLD.Get_rank()

            log = open(redirect, 'a')
            log.write('\nrank = %i, host = %s, system time = %s\n' %(rank, host, datetime.datetime.now()) )
            gb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024. * 1024.)
            log.write( 'CPU memory usage: %f GB\n' %gb)
            log.write('# Running os.system\n')
            log.write('%s\n' %(oscmd))
            log.close()
            os.system('%s >> %s 2>&1' %(oscmd, redirect))
            log = open(redirect, 'a')
            log.write('\nrank = %i, host = %s, system time = %s' %(rank, host, datetime.datetime.now()) )
            log.write('\n\n')
            log.close()
        
        elif kind=='popen':
            gb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024. * 1024.)
            redirect.info( 'CPU memory usage (resource): %f GB' %gb)

            #p = subprocess.Popen( ['ps', '-p', '%i' %os.getpid(), '-o', 'rss'], stdout=subprocess.PIPE, stderr=subprocess.PIPE )
            #stdout, stderr = p.communicate()
            #gb = float( stdout.split('\n')[1] ) / (1024. * 1024)
            #redirect.info( 'CPU memory usage (ps): %f GB' %gb)

            redirect.info( 'Running subprocess.Popen:' )
            redirect.info( subprocess.list2cmdline(oscmd) )
            p = subprocess.Popen(oscmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            redirect.info( 'Printing stdout:' )
            redirect.info(stdout)
            redirect.info( 'Printing stderr:' )
            redirect.info(stderr)


            redirect.info('\n')


# Download and uncompress images
def DownloadImages(indir, images, psfs, RunConfig, DerivedConfig, skip=False):
    useimages = []

    for file in images:
        infile = os.path.join(indir, os.path.basename(file))
        if not skip:
            Remove(infile)
            #subprocess.call( ['wget', '-q', '--no-check-certificate', file, '-O', infile] )
            #oscmd = ['wget', '-q', '--no-check-certificate', file, '-O', infile]
            oscmd = ['wget', '--no-check-certificate', file, '-O', infile]
            SystemCall(oscmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])
        ufile = infile.replace('.fits.fz', '.fits')
        if not skip:
            Remove(ufile) 
            #subprocess.call([RunConfig['funpack'], '-O', ufile, infile])
            oscmd = [RunConfig['funpack'], '-O', ufile, infile]
            SystemCall(oscmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])
        useimages.append(ufile)

    usepsfs = []
    for psf in psfs:
        pfile = os.path.join(indir, os.path.basename(psf))
        if not skip:
            Remove(pfile)
            #subprocess.call( ['wget', '-q', '--no-check-certificate', psf, '-O', pfile] )
            #oscmd = ['wget', '-q', '--no-check-certificate', psf, '-O', pfile]
            oscmd = ['wget', '--no-check-certificate', psf, '-O', pfile]
            SystemCall(oscmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])
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

    """
    for d in descr:
        name = d[0]
        cut = np.isnan(ndata[name])
        #ndata[name][cut] = RunConfig['DBnull']
        ndata[name][cut][:] = RunConfig['DBnull']

        '''
        if np.sum(cut) > 0:
            print 'replaced nan in %s' %(name)
        '''
    """

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
    #return '%s/%s@"(DESCRIPTION=(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%s))(CONNECT_DATA=(SERVER=%s)(SERVICE_NAME=%s)))"' %(cur.username,cur.password,db_specs['protocol'],db_specs['db_host'],db_specs['port'],db_specs['server'],db_specs['service_name'])
    return '%s/%s@"\(DESCRIPTION=\(ADDRESS=\(PROTOCOL=%s\)\(HOST=%s\)\(PORT=%s\)\)\(CONNECT_DATA=\(SERVER=%s\)\(SERVICE_NAME=%s\)\)\)"' %(cur.username,cur.password,db_specs['protocol'],db_specs['db_host'],db_specs['port'],db_specs['server'],db_specs['service_name'])



def AllDefs(arr, tablename):
    a = arr.view(np.ndarray)
    create_cmd, alldefs = desdb.desdb.get_tabledef(a.dtype.descr, tablename)
    return create_cmd, alldefs

def desdm_names(release):
    file = '%s_objects-columns.fits' %(release)
    cnames = pyfits.open(file)[1].data['column_name']
    names = []
    for c in cnames:
        names.append(c.strip().lower())
    return names


def UpdateCreates(arr, tablename, creates, names, j, i, singles, allbands, required='i', truth=False, release='y1a1_coadd', truth_cols=[]):
    #create_cmd = GetOracleStructure(arr, tablename, noarr=noarr, create=True)
    create_cmd, alldefs = AllDefs(arr, tablename)
    create_cmd = create_cmd.replace('not null', 'null')
    cc = create_cmd.split('\n')

    desdm_cols = desdm_names(release)
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
                creates, names = UpdateCreates(arr, tablename, creates, names, j, i, singles, allbands, required=required, truth=t, release=RunConfig['release'])

            else:
                dobj = UpdateInserts(arr, tablename, noarr, j, i, allbands, dobj, singles, valids, required=required, truth=t, release=RunConfig['release'])

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


def UpdateInserts(arr, tablename, noarr, j, i, allbands, dobj, singles, valids, required='i', truth=False, release='y1a1_coadd'):
    create_cmd, alldefs = AllDefs(arr, tablename)
    desdm_cols = desdm_names(release)
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
    return ['balrog_index', 'tilename', 'number_sex']

def OneOnly():
    return ['balrog_index', 'tilename',
            'x','y','g1','g2','magnification','halflightradius_0','beta_0','sersicindex_0','axisratio_0',
            'ra','dec','id','mod', 'objtype','z','indexstart','seed',
            'number_sex']


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
        arr = NewMakeOracleFriendly(cats[i], ext, BalrogConfig, DerivedConfig, labels[i], RunConfig)


        if RunConfig['DBload']=='sqlldr':
            s = sys.stdout
            e = sys.stderr
            log = open(DerivedConfig['desdblog'], 'a')
            sys.stdout = log
            sys.stderr = log

            print 'redirect print start time: %s' %(str(datetime.datetime.now()))
            controlfile = cat.replace('.fits', '')
            csvfile = cat.replace('.fits', '.csv')
            desdb.array2table(arr, tablename, controlfile, create=create)

            print 'redirect print end time: %s' %(str(datetime.datetime.now()))
            log.close()
            sys.stdout = s
            sys.seterr = e

            if create:
                create_file = '%s.create.sql' %(controlfile)
                create_cmd = open(create_file).read().strip()
                cur.quick(create_cmd)
            else:
                connstr = get_sqlldr_connection_info(DerivedConfig['db'])
                logfile = controlfile + '.sqlldr.log'

                oscmd = ['sqlldr', '%s' %(connstr), 'control=%s' %(controlfile), 'log=%s' %(logfile), 'silent=(header, feedback)']
                #SystemCall(oscmd, redirect=DerivedConfig['itlog'])
                SystemCall(oscmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])



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
    #subprocess.call(cmd)
    SystemCall(cmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])

    fixband = BalrogConfig['band']
    cats, labels, valids = GetRelevantCats2(BalrogConfig, RunConfig, DerivedConfig, allfix='det', create=True, appendsim=False, sim2nosim=False)

    NewWrite2DB2(cats, labels, valids, RunConfig, BalrogConfig, DerivedConfig)

    '''
    for i in range(len(DerivedConfig['bands'])):
        print fixband
        cats, labels = GetRelevantCatalogs(BalrogConfig, RunConfig, DerivedConfig, band=fixband, create=True)
        BalrogConfig['band'] = DerivedConfig['bands'][i]
        NewWrite2DB(cats, labels, RunConfig, BalrogConfig, DerivedConfig)
    '''


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
    #subprocess.call(cmd)
    SystemCall(cmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])

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

        '''
        cmd = Dict2Cmd(BConfig, RunConfig['balrog'])
        print cats, labels, valids
        print cmd
        SystemCall(cmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])
        '''

        #for k in range(len(DerivedConfig['images'])):
        for k in range(len(DerivedConfig['imbands'])):
            band = DerivedConfig['imbands'][k]
            BConfig['psf'] = DerivedConfig['psfs'][k]
            BConfig['image'] = DerivedConfig['images'][k]
            BConfig['band'] = band
            BConfig['outdir'] = os.path.join(DerivedConfig['outdir'], band)
            BConfig['zeropoint'] = GetZeropoint(RunConfig, DerivedConfig, BConfig)
            cmd = Dict2Cmd(BConfig, RunConfig['balrog'])
            SystemCall(cmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])

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
            #subprocess.call(cmd)

            SystemCall(cmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])

            #cats, labels, valid = GetRelevantCats2(BConfig, RunConfig, DerivedConfig)
            #NewWrite2DB2(cats, labels, valids, RunConfig, BConfig, DerivedConfig)

        #cats, labels, valids = GetRelevantCats2(BConfig, RunConfig, DerivedConfig)
        #NewWrite2DB2(cats, labels, valids, RunConfig, BConfig, DerivedConfig)

        cmd, detimage, detwimage = SwarpConfig(cimgs, RunConfig, DerivedConfig, BConfig)

        '''
        oscmd = subprocess.list2cmdline(cmd)
        swarplogfile = detimage.replace('.fits', '.log')
        swarplog = open(swarplogfile, 'w')
        swarplog.write('# Exact command call\n')
        swarplog.write('%s\n' %(oscmd))
        swarplog.close()
        os.system('%s >> %s 2>&1' %(oscmd, swarplogfile))
        '''
        SystemCall(cmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])



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


        #runlog.info('%s %s %s %s' %('h', BConfig['band'], DerivedConfig['iteration'], socket.gethostname()))
        cmd = Dict2Cmd(BConfig, RunConfig['balrog'])
        #subprocess.call(cmd)
        SystemCall(cmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])
        #runlog.info('%s %s %s %s' %('j', BConfig['band'], DerivedConfig['iteration'], socket.gethostname()))

        #cats, labels = GetRelevantCatalogs(BConfig, RunConfig, DerivedConfig, appendsim=appendsim)
        #NewWrite2DB(cats, labels, RunConfig, BConfig, DerivedConfig)

    BConfig['nodraw'] = False
    cats, labels, valids = GetRelevantCats2(BConfig, RunConfig, DerivedConfig, appendsim=True)
    NewWrite2DB2(cats, labels, valids, RunConfig, BConfig, DerivedConfig)


def RunNormal(RunConfig, BalrogConfig, DerivedConfig):
    
    coordfile = WriteCoords(DerivedConfig['pos'], DerivedConfig['outdir'])
    BalrogConfig['poscat'] = coordfile
    if RunConfig['dualdetection']!=None:

        BConfig = copy.copy(BalrogConfig)
        BConfig['imageonly'] = False
        BConfig['image'] = DerivedConfig['images'][0]
        BConfig['psf'] = DerivedConfig['psfs'][0]
        BConfig['band'] = DerivedConfig['bands'][0]
        BConfig['zeropoint'] = GetZeropoint(RunConfig, DerivedConfig, BConfig)
        BConfig['outdir'] = os.path.join(DerivedConfig['outdir'], BConfig['band'])
        BConfig['nodraw'] = True
        BConfig['nonosim'] = True

        cmd = Dict2Cmd(BConfig, RunConfig['balrog'])
        SystemCall(cmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])

        cats, labels, valids = GetRelevantCats2(BConfig, RunConfig, DerivedConfig, sim2nosim=True)
        NewWrite2DB2(cats, labels, valids, RunConfig, BConfig, DerivedConfig)

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
            #subprocess.call(cmd)
            SystemCall(cmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])

            cats, labels, valid = GetRelevantCats2(BConfig, RunConfig, DerivedConfig)
            NewWrite2DB2(cats, labels, valids, RunConfig, BConfig, DerivedConfig)

        cmd, detimage, detwimage = SwarpConfig(cimgs, RunConfig, DerivedConfig, BConfig)

        '''
        oscmd = subprocess.list2cmdline(cmd)
        swarplogfile = detimage.replace('.fits', '.log')
        swarplog = open(swarplogfile, 'w')
        swarplog.write('# Exact command call\n')
        swarplog.write('%s\n' %(oscmd))
        swarplog.close()
        os.system('%s >> %s 2>&1' %(oscmd, swarplogfile))
        '''
        SystemCall(cmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])

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
            BConfig['nonosim'] = True
            
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
        #subprocess.call(cmd)
        SystemCall(cmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])
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
        RunNormal2(RunConfig, BalrogConfig, DerivedConfig)

    if RunConfig['intermediate-clean']:
        if it < 0:
            #subprocess.call( ['rm', '-r', BalrogConfig['outdir']] )
            oscmd = ['rm', '-r', BalrogConfig['outdir']]
            SystemCall(oscmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])
        else:
            for band in DerivedConfig['bands']:
                dir = os.path.join(DerivedConfig['outdir'], band)
                #subprocess.call( ['rm', '-r', dir] )
                oscmd = ['rm', '-r', dir]
                SystemCall(oscmd, redirect=DerivedConfig['itlog'], kind=RunConfig['command'])



#lock = Lock()

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


    DerivedConfig['images'], DerivedConfig['psfs'] = DownloadImages(DerivedConfig['indir'], DerivedConfig['images'], DerivedConfig['psfs'], RunConfig, DerivedConfig, skip=DerivedConfig['initialized'])


    if (DerivedConfig['iteration']!=-2) and (DerivedConfig['initialized']==False):
        send = -3
        MPI.COMM_WORLD.sendrecv([rank,host,send], dest=0, source=0)

    run_balrog( [RunConfig, BalrogConfig, DerivedConfig] )
