
class RunConfigurations:

        default = {
            'joblabel': 'test',
            'nodes': 10,
            'ppn': 6,
            'runnum': 0,

            # Relevant only for NERSC
            'walltime': '24:00:00',

            'balrog': 'balrog',  # Balrog executable you'll use
            'wget': 'wget', # wget executable
            'funpack': 'funpack', # funpack executable 
            'swarp': 'swarp', # swarp executable, only relevant if in multi-image detection mode
            'swarp-config': 'default.swarp', # swarp configuration file, only relevant if in multi-image detection mode

            'release': 'sva1_coadd',
            'db-columns': 'sva1_coadd_objects-columns.fits',
            'dbname': 'debug',  # DB tables will look like <username>.balrog_<label>_<type>_<band>

            'outdir': None,  # The ouput directory for all intermediate work. This should be in the scratch area on the node.
            'jobdir': None,  # The ouput directory for all job files and job logs. This should be in the scratch area on the node.
            'pos': 'tilepos',

            'fixwrapseed': None, # Fix this to an integer to get the same Balrog sampling realizations each time you run
            'fixnoiseseed': None # Fix this to an integer to get the same Balrog noise realizations each time you run
        }



class BalrogConfigurations:

        default = {
            'fulltraceback': True,
            'ngal': 1000,
            'pyconfig': 'config.py',

            'catfitstype': 'ldac',
            'sexnnw': 'sex.nnw',
            'sexconv': 'sex.conv',
            'sexparam': 'sex.param_diskonly',
            'nosimsexparam': 'sex.param_diskonly',
            'sexconfig': 'sex.config',
            'sexpath': 'sex'
        }


class DBInfo:

    default = {
        'db_host' : 'leovip148.ncsa.uiuc.edu',
        'protocol' : 'TCP',
        'port' : '1521',
        'server' : 'dedicated',
        'service_name' : 'dessci',
    }


