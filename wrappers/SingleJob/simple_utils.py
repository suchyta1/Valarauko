import os

class db_specs:
    file_host = 'desar2.cosmology.illinois.edu'
    file_prefix = '/DESFiles/desardata'
    file_archive = 'https://%s:%s' %(file_host,file_prefix)

    db_host = 'leovip148.ncsa.uiuc.edu'
    protocol = 'TCP'
    port = '1521'
    server = 'dedicated'
    service_name = 'dessci'


def get_sqlldr_connection_info():
    user, password = retrieve_login(db_specs.db_host)
    return '%s/%s@"(DESCRIPTION=(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%s))(CONNECT_DATA=(SERVER=%s)(SERVICE_NAME=%s)))"' %(user,password,db_specs.protocol,db_specs.db_host,db_specs.port,db_specs.server,db_specs.service_name) 


def retrieve_login(host):
    netrc = os.path.join( os.environ['HOME'], '.netrc' )
    lines = open(netrc).read().strip().split('\n')
    for line in lines:
        if line=='':
            break
        line = line.strip()
        if line == '':
            break
        words = line.split()
        machine = words[1]
        user = words[3]
        pwd = words[5]
        if machine==host:
            return [user, pwd]

    raise Exception("Could not get proper credentials. Check .netrc setup")

