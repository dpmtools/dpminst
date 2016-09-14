#
# Copyright (C) 2016  Xu Tian <tianxu@iscas.ac.cn>
# Licensed under The MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os
import sys
import tempfile
import commands
from lib.loadconf import load_conf
from lib.util import sshpass, scp_to
from conf.dpminst import DB_SERVERS
from conf.dpm.mongodb import MONGO_PORT

_name = commands.getoutput('readlink -f %s' % sys.argv[0])
_path = os.path.dirname(_name)

def install_mongo():
    buf = []
    addr = '0.0.0.0'
    path = os.path.join(os.path.dirname(_path), 'conf', 'mongodb', 'mongodb.conf')
    with open(path) as f:
        lines = f.readlines()
        for i in range(len(lines)):
            if 'bind_ip' not in lines[i] and 'port' not in lines[i]:
                buf.append(lines[i])
            elif 'bind_ip' in lines[i]:
                name, _ = lines[i].split('=')
                name = name.strip()
                buf.append('%s = %s\n' % (name, addr))
            elif 'port' in lines[i]:
                name = lines[i].split('=')
                name = name[0].strip()
                if 'port' == name:
                    buf.append('%s = %s\n' % (name, MONGO_PORT))
    
    path =  tempfile.mktemp()
    try:
        with open(path, 'w') as f:
            f.writelines(buf)
        current = []
        servers =  load_conf(DB_SERVERS)
        for role in DB_SERVERS:
            for host in servers[role]:
                if host not in current:
                    sshpass(host, 'apt-get install -y mongodb')
                    scp_to(host, path, '/etc/mongodb.conf')
                    sshpass(host, 'service mongodb restart')
                    current.append(host)
    finally:
        if os.path.exists(path):
            os.remove(path)
