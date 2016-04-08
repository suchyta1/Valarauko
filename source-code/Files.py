import os

class Files:
    substr = 'subjob'
    depstr = 'dep'
    startupfile = 'startupfile'
    cok = 'createok'
    cfail = 'createfail'
    dupok = 'dupok'
    dupfail = 'dupfail'
    exit = 'exit'
    anyfail = 'anyfail'
    json = 'config.json'
    runlog = 'runlog'


def GetJsonDir(run, dirname, id):
    '''
    if (run['nodes'] > 1) or (run['asarray']):
        jdir = os.path.join(dirname, '%s_%i'%(Files.substr,id))
    else:
        jdir = dirname
    return jdir
    '''
    return os.path.join(dirname, '%s_%i'%(Files.substr,id))
