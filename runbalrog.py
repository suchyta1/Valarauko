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



def Run(args):
    bargs = args[0]
    subprocess.call(bargs)

    imagein,out_files,out_exts,control_files,table_names,index,create,tile = args[1]
    Write2DB(out_files, out_exts, control_files, table_names, create, index,tile)


def MakeOracleFriendly(file, ext, create, index, tile):
    if file.find('measuredcat')!=-1 and (index!=-1):
        arr = TweakTable(file, ext, assoc=True)

    elif (index==-1):
        xhdu = pyfits.open(file)[ext]
        data = xhdu.data
        length = len(data)
        t = np.array([tile]*length)
        extras = [('tile', '|S12', 'None', t)]
        if create:
            arr = TweakTable(file, ext, create=True, extras=extras)
        else:
            arr = TweakTable(file, ext, extras=extras)

    else:
        hdu = pyfits.open(file)[ext]
        arr = np.array(hdu.data)

    return arr


def TweakTable(file, ext, assoc=False, extras=[], index_key='balrog_index', create=False):
    xhdu = pyfits.open(file)[ext]
    data = xhdu.data

    ndata = np.array(data)
    dtype = []

    cols = []
    if assoc:
        pos = None
        for name in xhdu.header.keys():
            if xhdu.header[name] == index_key:
                pos = int(name[1:])
                break
        
        if pos!=None:
            index = ndata['VECTOR_ASSOC'][:, pos]
            ndata = recfunctions.append_fields(ndata, index_key, index, usemask=False)
            ndata = recfunctions.drop_fields(ndata, 'VECTOR_ASSOC', usemask=False)

    if create:
        ndata = recfunctions.drop_fields(ndata, 'VECTOR_ASSOC', usemask=False)

    names = np.array( ndata.dtype.names )
    dtype = []
    for i in range(len(names)):
        if names[i]=='NUMBER':
            dt = ('NUMBER_SEX', ndata.dtype.descr[i][1])
        else:
            dt = ndata.dtype.descr[i]
        dtype.append(dt)
    ndata.dtype = dtype

    for extra in extras:
        ndata = recfunctions.append_fields(ndata, extra[0], data=extra[3], dtypes=extra[1], usemask=False)


    return ndata


def get_sqlldr_connection_info(db_specs):
    cur = desdb.connect()
    return '%s/%s@"(DESCRIPTION=(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%s))(CONNECT_DATA=(SERVER=%s)(SERVICE_NAME=%s)))"' %(cur.username,cur.password,db_specs['protocol'],db_specs['db_host'],db_specs['port'],db_specs['server'],db_specs['service_name']) 


def Write2DB(out_files, out_exts, control_files, table_names, create, index, tile):
    connstr = get_sqlldr_connection_info()
    for i in range(len(out_files)):
        csv_file = '%s.csv' %(control_files[i])
        arr = MakeOracleFriendly(out_files[i], out_exts[i], create, index, tile)
        desdb.array2table(arr, table_names[i], control_files[i], create=create)

        if create:
            cur = desdb.connect()
            create_file = '%s.create.sql' %(control_files[i])
            create_cmd = open(create_file).read().strip()
            cur.quick(create_cmd)
            cur.quick("GRANT SELECT ON %s TO DES_READER" %table_names[i])

        else:
            log_file = control_files[i] + '.sqlldr.log'
            print 'pushing %s' %(control_files[i]); sys.stdout.flush()
            subprocess.call(['sqlldr', 
                             '%s' %(connstr),
                             'control=%s' %(control_files[i]),
                             'log=%s' %(log_file),
                             'silent=(header, feedback)'])
                             #'parallel=true',
                             #'direct=true',
                             #'skip_index_maintenance=true'])
            print 'done pushing %s' %(control_files[i]); sys.stdout.flush()
        
        
def GetOpts():
    parser = argparse.ArgumentParser()

    parser.add_argument( "-od", "--outdir", help="Output top directory", type=str, required=True)
    parser.add_argument( "-t", "--tile", help="Tilename", type=str, required=True)
    parser.add_argument( "-b", "--band", help='Filter band', type=str, required=True)
    parser.add_argument( "-l", "--label", help='Output label', type=str, required=True)
    parser.add_argument( "-cr", "--create", help="Create DB", action="store_true" )
    parser.add_argument( "-tb", "--tables", help='Table names', type=str, required=True)
    parser.add_argument( "-ft", "--fitstype", help='FITS sextractor type', type=str, default='ldac', choices=['ldac','1.0'])

    parser.add_argument( "-idir", "--imagedir", help="Input image diretory", type=str, required=True)
    #parser.add_argument( "-tis", "--tileindexstart", help="(Balrog) Index for first simulated galaxy of the tile", type=int, required=True)
    parser.add_argument( "-ti", "--tileindex", help="(Balrog) Index for the tile", type=int, required=True)
    parser.add_argument( "-p", "--pyconfig", help='pyconfig file', type=str, required=True )

    parser.add_argument( "-fc", "--fullclean", help="Full Clean", action="store_true" )
    parser.add_argument( "-cl", "--clean", help="Clean", action="store_true" )
    parser.add_argument( "-c", "--compressed", help="Images are compressed", action="store_true" )

    parser.add_argument( "-ng", "--ngal", help="Number of simulated galaxies per simulation", type=int, default=1000)
    parser.add_argument( "-nt", "--ntot", help="Total number of simulated galaxies. This will determine how many simulations to run", type=int, default=500000)
    #parser.add_argument( "-sd", "--seed", help="Rng seed", type=int, required=True)
    parser.add_argument( "-kp", "--kappa", help="kappa", type=float, default=0.0)

    parser.add_argument( "-ps", "--presex", help="Run sextractor on DESDM tile as index -1", action="store_true" )
    parser.add_argument( "-spp", "--sexpath", help='sextractor path', type=str, required=True)
    parser.add_argument( "-sn", "--sexnnw", help='sex nnw', type=str, required=True )
    parser.add_argument( "-sf", "--sexconv", help='sex conv', type=str, required=True )
    parser.add_argument( "-sp", "--sexparam", help='sex param', type=str, required=True )
    parser.add_argument( "-sc", "--sexconfig", help='sex param', type=str, required=True )

    opts = parser.parse_args()
    return opts


def CommonBargs(opts, outdir, indexstart, posstart, seed, ngal):
    #balrog = os.path.join(os.environ['BALROG_DIR'], 'balrog.py')
    balrog = '/astro/u/esuchyta/git_repos/balrog-testing/Balrog/balrog.py'
    bargs = [balrog, 
             '--outdir', outdir, 
             '--tile', opts.tile,
             '--image', opts.imagein, 
             '--imageext', str(opts.imageext),
             '--psf', opts.psfin, 
             '--ngal', str(ngal), 
             '--indexstart', str(indexstart), 
             '--posstart', str(posstart),
             '--seed', str(seed),
             '--band', opts.band,

             #'--kappa', str(opts.kappa),
             '--poscat', opts.coordfile,
             #'--magimage', opts.magimage,
             #'--ramin', str(opts.ramin),
             #'--decmin', str(opts.decmin),
             
             #'--nmin', opts.nmin,
             #'--nmax', opts.nmax,
             #'--dn', opts.dn,
             #'--ntype', opts.ntype,

             '--sexpath', opts.sexpath, 
             '--sexnnw', opts.sexnnw, 
             '--sexconv', opts.sexconv, 
             '--sexparam', opts.sexparam, 
             '--nosimsexparam', opts.sexparam,
             '--sexconfig', opts.sexconfig, 
             '--pyconfig', opts.pyconfig, 
             '--catfitstype', opts.fitstype,
             '--fulltraceback']
    if opts.clean:
        bargs.append('--clean')
    return bargs


def BalrogExtras(opts, outdir, tnames, i):
    out_truth = os.path.join(outdir, 'balrog_cat', '%s_%s.truthcat.sim.fits'%(opts.tile,opts.band))
    out_nosim = os.path.join(outdir, 'balrog_cat', '%s_%s.measuredcat.nosim.fits'%(opts.tile,opts.band))
    out_sim = os.path.join(outdir, 'balrog_cat', '%s_%s.measuredcat.sim.fits'%(opts.tile,opts.band))
    out_files = [out_truth, out_nosim, out_sim]
    if opts.fitstype == 'ldac':
        out_exts = [1,2,2]
    else:
        out_exts = [1,1,1]
    table_names = tnames[0:3]
    control_files = []
    for file in out_files:
        control_files.append( file.replace('.fits', '') )
    return GetExtras(opts, out_files, out_exts, control_files, table_names, i)


def DesExtras(opts, outdir, tnames):
    out_des = os.path.join(outdir, 'balrog_cat', '%s_%s.measuredcat.sim.fits'%(opts.tile,opts.band))
    out_files = [out_des]
    if opts.fitstype == 'ldac':
        out_exts = [2]
    else:
        out_exts = [1]
    table_names = tnames[3:]
    control_files = []
    for file in out_files:
        control_files.append( file.replace('.fits', '') )
    return GetExtras(opts, out_files, out_exts, control_files, table_names, -1)


def GetExtras(opts, out_files, out_exts, control_files, table_names, i):
    extra = []
    extra.append(opts.imagein)
    extra.append(out_files)
    extra.append(out_exts)
    extra.append(control_files)
    extra.append(table_names)
    extra.append(i)
    extra.append(opts.create)
    extra.append(opts.tile)
    return extra


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def RunBalrog(common_config, tile_config, band_config):
    opts = copy.copy(common_config)
    for key in tile_config.keys():
        opts[key] = tile_config[key]
    for key in band_config.keys():
        opts[key] = band_config[key]

    opts['ntot'] = len(opts['tilecoords'])
    tnames = opts['tnames']

    bools = ['create', 'presex', 'compressed', 'clean', 'fullclean']
    for bool in bools:
        if bool not in opts.keys():
            opts[bool] = False

    opts = AttrDict(opts)


    coorddir = os.path.join(opts.outdir, '%s_coords'%(opts.label), opts.tile)
    if not os.path.lexists(coorddir):
        os.makedirs(coorddir)
  
    opts.coordfile = os.path.join(coorddir, '%s.fits' %(opts.band))
    if opts.ngal > 0:
        xcol = pyfits.Column(name='x', format='D', unit='pix', array=opts.tilecoords[:,0])
        ycol = pyfits.Column(name='y', format='D', unit='pix', array=opts.tilecoords[:,1])
        rcol = pyfits.Column(name='ra', format='D', unit='deg', array=opts.wcoords[:,0])
        dcol = pyfits.Column(name='dec', format='D', unit='deg', array=opts.wcoords[:,1])
    else:
        xcol = pyfits.Column(name='x', format='D', unit='pix', array=[])
        ycol = pyfits.Column(name='y', format='D', unit='pix', array=[])
        rcol = pyfits.Column(name='ra', format='D', unit='deg', array=[])
        dcol = pyfits.Column(name='dec', format='D', unit='deg', array=[])
    columns = [xcol, ycol, rcol, dcol]
    tbhdu = pyfits.BinTableHDU.from_columns(pyfits.ColDefs(columns))
    phdu = pyfits.PrimaryHDU()
    hdus = pyfits.HDUList([phdu,tbhdu])
    if os.path.lexists(opts.coordfile):
        os.remove(opts.coordfile)
    hdus.writeto(opts.coordfile)

    opts.logdir = os.path.join(opts.outdir, '%s_log'%(opts.label), opts.tile)
    if not os.path.lexists(opts.logdir):
        os.makedirs(opts.logdir)

    logfile = os.path.join(opts.logdir, '%s.log.txt' %(opts.band))
    logobj = StringIO.StringIO()
    sys.stdout = logobj
    sys.stderr = logobj

    opts.imageext = 0
    opts.psfin = os.path.join(opts.imagedir, '%s_%s_psfcat.psf' %(opts.tile, opts.band))
    if opts.compressed:
        opts.imagein = os.path.join(opts.imagedir, '%s_%s.fits.fz' %(opts.tile, opts.band))

        opts.tmpdir = os.path.join(opts.outdir, '%s_uncompress'%(opts.label), opts.tile)
        if not os.path.lexists(opts.tmpdir):
            os.makedirs(opts.tmpdir)
        tmpimage = os.path.join(opts.tmpdir, '%s_%s.fits' %(opts.tile, opts.band))
        if not os.path.lexists(tmpimage):
            subprocess.call(['funpack', '-O', tmpimage, opts.imagein])
        opts.imagein = tmpimage

    else:
        opts.imagein = os.path.join(opts.imagedir, '%s_%s.fits' %(opts.tile, opts.band))

    if (opts.create) and (opts.ntot==0):
        opts.ngal = 0
        nsim = 1
    else:
        nsim = opts.ntot / opts.ngal
        if opts.ntot % opts.ngal != 0:
            nsim += 1
        nsim = int(nsim)

    args = []

    if opts.presex:
        outdir = os.path.join(opts.outdir, opts.label, opts.tile, opts.band, str(-1))
        bargs = CommonBargs(opts, outdir, 0, 0, 0, 0)
        bargs.append('--nonosim')
        if not opts.create:
            bargs.append('--noassoc')
        extra = DesExtras(opts, outdir, tnames)
        args.append([bargs,extra])

    for i in range(nsim):
        outdir = os.path.join(opts.outdir, opts.label, opts.tile, opts.band, str(i))
        #indexstart = opts.tileindex*opts.ntot + opts.ngal*i
        indexstart = opts.indexstart + opts.ngal*i
        posstart = opts.ngal*i
        seed = opts.indexstart + i

        if i == (nsim - 1):
            num = opts.ntot - i*opts.ngal
            bargs = CommonBargs(opts, outdir, indexstart, posstart, seed, num)
        else:
            bargs = CommonBargs(opts, outdir, indexstart, posstart, seed, opts.ngal)

        extra = BalrogExtras(opts, outdir, tnames, i)
        args.append([bargs,extra])


    t1 = datetime.datetime.now()
    print 'common_config = %s' %(str(common_config))
    print 'tile_config = %s' %(str(tile_config))
    print 'band_config = %s' %(str(band_config))
    print 'Started time: %s' %(str(t1)); sys.stdout.flush()

    nthreads = cpu_count()
    pool = Pool(nthreads)
    pool.map(Run, args)
    #Run(args[0])
    #Run(args[1])
    
    if opts.fullclean:
        outdir = os.path.join(opts.outdir, opts.label, opts.tile, opts.band)
        subprocess.call(['rm', '-r', outdir])
        #os.remove(opts.coordfile)
        #subprocess.call(['rm', logfile])
        if opts.compressed:
            subprocess.call(['rm', tmpimage])


    t2 = datetime.datetime.now()
    print 'Completed time: %s' %(str(t2)); sys.stdout.flush()
    print 'Elapsed time: %s' %(str(t2-t1)); sys.stdout.flush()

    sys.stdout = sys.__stdout__
    outlog = logobj.getvalue()
    f = open(logfile, 'w')
    f.write(outlog)
    f.close()


#####################################################################################


def Remove(file):
    if os.path.lexists(file):
        os.remove(file)

def Mkdir(dir):
    if not os.path.lexists(dir):
        os.makedirs(dir)


def DownloadImages(indir, images, psfs, skip=False):
    useimages = []

    time1 = datetime.datetime.now()
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
    time2 = datetime.datetime.now()
    #print (time2-time1).total_seconds()

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


def GetDetStuff(BalrogConfig, RunConfig, images, ext=0, zpkey='SEXMGZPT'):
    index = np.array( RunConfig['dualdetection'] )
    bands =  np.array(RunConfig['bands'])
    BalrogConfig['detbands'] = ','.join(bands[index] )
    
    zps = []
    for i in index:
        num = i + 1
        header = pyfits.open(images[num])[ext].header
        zp = header[zpkey]
        zps.append(str(zp))
    BalrogConfig['detzeropoints'] = ','.join(zps)
    return BalrogConfig


def DoBandStuff(BalrogConfig, RunConfig, band, images, ext=0, zpkey='SEXMGZPT'):
    BalrogConfig['band'] = band
    if band=='det':
        BalrogConfig = GetDetStuff(BalrogConfig, RunConfig, images, ext=ext, zpkey=zpkey)
    return BalrogConfig


def GetSeed(BalrogConfig, DerivedConfig):
    BalrogConfig['seed'] = BalrogConfig['indexstart'] + DerivedConfig['seedoffset']
    return BalrogConfig


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
        arr = MakeOracleFriendly(cats[i], ext, create, it, BalrogConfig['tile'])


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
                log_file = controlfile + '.sqlldr.log'
                print 'pushing %s' %(controlfile); sys.stdout.flush()
                subprocess.call(['sqlldr', '%s' %(connstr), 'control=%s' %(control_files[i]), 'log=%s' %(log_file), 'silent=(header, feedback)'])
                print 'done pushing %s' %(control_files[i]); sys.stdout.flush()


        elif RunConfig['DBload']=='cx_Oracle':
            noarr = False
            if labels[i]=='truth':
                noarr = True

            if create:
                create_cmd = GetOracleStructure(arr, tablename, noarr=noarr, create=True)
                cur.quick(create_cmd)
            else:
                istr, newarr = GetOracleStructure(arr, tablename, noarr=noarr)
                cxcur = get_cx_oracle_cursor(DerivedConfig['db'])
                '''
                for row in range(len(newarr)):
                    newarr[i] = tuple(newarr[i])
                '''
                #print newarr
                print istr
                print len(newarr), len(newarr[0])
                cxcur.prepare(istr)
                cxcur.executemany(None, newarr)
                #cxcur.executemany(istr, newarr)

        if create:
            cur.quick("GRANT SELECT ON %s TO DES_READER" %tablename)



def get_cx_oracle_cursor(db_specs):
    c = desdb.connect()
    connection = cx_Oracle.connect( "%s/%s@(DESCRIPTION=(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%s))(CONNECT_DATA=(SERVER=%s)(SERVICE_NAME=%s)))" %(c.username,c.password,db_specs['protocol'],db_specs['db_host'],db_specs['port'],db_specs['server'],db_specs['service_name']) )
    cur = connection.cursor()
    return cur

def MakeNewArray(alldefs, arr, tablename, noarr=False):
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
        else:
            n = name[ : (isarr.span()[0]) ]
            j = int( isarr.group(0)[1:] ) - 1
            cols.append("arr['%s'][:,%i]"%(n,j) )
        names.append(name)
        nums.append(':%i'%(i+1) )
    colstr = ', '.join(cols)
    numstr = ', '.join(nums)
    namestr = ', '.join(names)
    print len(cols), len(nums), len(names)
    estr = "newarr = np.dstack( (%s) )[0]" %(colstr)
    istr = "insert into %s (%s) values (%s)" %(tablename, namestr, numstr)
    exec estr
    return istr, newarr.tolist()


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
    rcol = pyfits.Column(name='ra', format='D', unit='deg', array=opts.coords[:,0])
    dcol = pyfits.Column(name='dec', format='D', unit='deg', array=opts.coords[:,1])
    columns = [rcol, dcol]
    tbhdu = pyfits.BinTableHDU.from_columns(pyfits.ColDefs(columns))
    phdu = pyfits.PrimaryHDU()
    hdus = pyfits.HDUList([phdu,tbhdu])
    if os.path.lexists(coordfile):
        os.remove(coordfile)
    hdus.writeto(coordfile)
    return coordfile


def RunOnlyCreate(RunConfig, BalrogConfig, DerivedConfig):
    BalrogConfig['ngal'] = 0
    BalrogConfig['image'] = DerivedConfig['images'][0]
    BalrogConfig['psf'] = DerivedConfig['psfs'][0]
    BalrogConfig['indexstart'] = DerivedConfig['indexstart'] + DerivedConfig['iteration']*BalrogConfig['ngal']
    BalrogConfig = GetSeed(BalrogConfig, DerivedConfig)
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
    BalrogConfig['indexstart'] = DerivedConfig['indexstart'] + DerivedConfig['iteration'][0]*BalrogConfig['ngal']
    BalrogConfig = GetSeed(BalrogConfig, DerivedConfig)
    BalrogConfig = DoBandStuff(BalrogConfig, RunConfig, DerivedConfig['bands'][0], DerivedConfig['images'])
    BalrogConfig['outdir'] = os.path.join(DerivedConfig['outdir'], BalrogConfig['band'])
    if RunConfig['dualdetection']!=None:
        BalrogConfig['detimage'] = DerivedConfig['images'][0]
        BalrogConfig['detpsf'] = DerivedConfig['psfs'][0]

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
        RunNormal()

def NewRunBalrog(RunConfig, BalrogConfig, DerivedConfig):
    workingdir = os.path.join(RunConfig['outdir'], RunConfig['label'], DerivedConfig['tile'] )
    indir = os.path.join(workingdir, 'input')
    Mkdir(indir)

    if RunConfig['fixwrapseed'] != None:
        DerivedConfig['seedoffset'] = RunConfig['fixwrapseed']
    else:
        DerivedConfig['seedoffset'] = np.random.randint(10000)


    DerivedConfig['images'], DerivedConfig['psfs'] = DownloadImages(indir, DerivedConfig['images'], DerivedConfig['psfs'], skip=True)
    BalrogConfig['tile'] = DerivedConfig['tile']
    DerivedConfig['bands'] = PrependDet(RunConfig)

    args = []
    for it in DerivedConfig['iterations']:
        outdir = os.path.join(workingdir, 'output', '%i'%it)
        Mkdir(outdir)
        
        BConfig = copy.copy(BalrogConfig)
        DConfig = copy.copy(DerivedConfig)
        DConfig['iteration'] = it
        DConfig['outdir'] = outdir

        if it==-2:
            DConfig['pos'] = None
            arg = [RunConfig, BConfig, DConfig]
            args.append(arg)

        elif it==-1:
            DConfig['pos'] = None
            for i in range(len(DerivedConfig['bands'])):
                DDConfig = copy.copy(DConfig)
                DDConfig['iteration'] = (it,i)
                arg = [RunConfig, BalrogConfig, DDConfig]
                args.append(arg)
        else:
            DConfig['pos'] = DerivedConfig['pos'][it]
            arg = [RunConfig, BalrogConfig]
            args.append(arg)
   
    nthreads = cpu_count()
    pool = Pool(nthreads)
    run_balrog(args[0])
    #pool.map(run_balrog, args, chunksize=1)


