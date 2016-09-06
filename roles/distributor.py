#
# Copyright (C) 2016 Xu Tian <tianxu@iscas.ac.cn>
# Licensed under The MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os
import sys
import shutil
import tempfile
import commands
from threading import Thread
from lib.loadconf import load_conf, load_port
from lib.util import scp_to, sshpass, sshpass_output
from conf.dpminst import DB_SERVERS, FS_SERVERS, DPM_SERVERS, PORT_NAMES, DPM_URL, PATH_INST

SERVERS = DB_SERVERS + FS_SERVERS + DPM_SERVERS
FILES = ['category.py', 'config.py', 'hadoop.py', 'mongodb.py', 'path.py']

_name = commands.getoutput('readlink -f %s' % sys.argv[0])
_path = os.path.dirname(_name)

def _gen_conf(dirname):
    src_dir = os.path.join(os.path.dirname(_path), 'conf')
    dest_dir = os.path.join(dirname, 'conf')
    
    for name in FILES:
        src = os.path.join(src_dir, 'dpm', name)
        shutil.copy(src, dest_dir)

def _gen_servers(dirname, servers):
    filename = os.path.join(dirname, 'conf', 'servers.py')
    with open(filename, 'w') as f:
        for portname in PORT_NAMES:
            port = load_port(portname)
            f.write('%s = %s\n' % (portname, str(port)))
        for role in servers:
            f.write('%s = %s\n' % (role, str(servers[role])))

def _mkdtemp():
    path = tempfile.mkdtemp()
    os.system('git clone -b upstream %s %s' % (DPM_URL, os.path.join(path, 'dpm')))
    return path

def _compress(dirname):
    path = tempfile.mktemp()
    src = os.path.dirname(dirname)
    basename = os.path.basename(dirname)
    cmd = 'cd %s;tar zcf %s %s' % (src, path, basename)
    os.system(cmd)
    return path

def _distribute(dpmfile, host, role):
    target = os.path.join(PATH_INST, 'dpm', 'install')
    test_dep = os.path.join(PATH_INST, 'dpm', 'tests', 'test-dep')
    
    scp_to(host, dpmfile, dpmfile)
    sshpass(host, 'mkdir -p %s;rm -rf %s*' % (PATH_INST, os.path.join(PATH_INST, 'dpm')))
    sshpass(host, 'tar zxf %s --directory %s 2>/dev/null' % (dpmfile, PATH_INST))
    sshpass(host, 'rm %s' % dpmfile)
    
    output = None
    if role == 'SERVER_BACKEND':
        sshpass(host, '%s -b' % target)
        output = sshpass_output(host, '%s -b' % test_dep)
    elif role == 'SERVER_INSTALLER':
        sshpass(host, '%s -i' % target)
        output = sshpass_output(host, '%s -i' % test_dep)
    elif role == 'SERVER_MANAGER':
        sshpass(host, '%s -m' % target)
        output = sshpass_output(host, '%s -m' % test_dep)
    elif role == 'SERVER_REPOSITORY':
        sshpass(host, '%s -r' % target)
        output = sshpass_output(host, '%s -r' % test_dep)  
    else:
        sshpass(host, target) 
    if output:
        if not output.startswith('bash: warning:'):
            raise Exception('%s, failed to perform test' % host)

def distribute():
    path = None
    dpmfile = None
    try:
        path =  _mkdtemp()
        servers = load_conf(SERVERS)
        dirname = os.path.join(path, 'dpm')
        _gen_servers(dirname, servers)
        _gen_conf(dirname)
        dpmfile = _compress(dirname)
        
        threads = []
        for role in DPM_SERVERS:
            for host in servers[role]:
                th = Thread(target=_distribute, args=(dpmfile, host, role))
                threads.append(th)
                th.start()
        
        for th in threads:
            th.join()
    finally:
        if path:
            shutil.rmtree(path)
        if dpmfile:
            os.remove(dpmfile)
