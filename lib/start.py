#
# Copyright (C) 2016  Xu Tian <tianxu@iscas.ac.cn>
# Licensed under The MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os
import time
from lib.loadconf import load_conf
from roles.hadoop import get_name
from lib.loadconf import _extract_addr
from lib.util import sshpass, sshpass_output
from conf.dpm.mongodb import MONGO_PORT
from conf.hadoop.config import HDFS_CLUSTERS
from conf.dpminst import DB_SERVERS, DPM_SERVERS, PATH_INST

RETRY = 40
WAITTIME = 5 # seconds
ROLES = [
               'SERVER_BACKEND',
               'SERVER_MANAGER',
               'SERVER_FRONTEND',
               'SERVER_RECORDER',
               'SERVER_REPOSITORY']

def start_dpm():
    servers = load_conf(DPM_SERVERS)
    stop_dpm = os.path.join(PATH_INST, 'dpm', 'bin', 'dpm-stop')
    start_dpm = os.path.join(PATH_INST, 'dpm', 'bin', 'dpm-start')
    for role in ROLES:
        for host in servers[role]:
            sshpass(host, stop_dpm)
            sshpass(host, start_dpm, background=True)
    if 'SERVER_ALLOCATOR' not in servers or 'SERVER_INSTALLER' not in servers:
        raise Exception('invalid servers, failed to start allocator and installer')
    else:
        for host in servers['SERVER_ALLOCATOR']:
            sshpass(host, stop_dpm)
            sshpass(host, start_dpm, background=True)
        time.sleep(30)
        for host in servers['SERVER_INSTALLER']:
            sshpass(host, stop_dpm)
            sshpass(host, start_dpm, background=True)

def _check_hadoop():
    for cluster in HDFS_CLUSTERS:
        namenode = cluster['namenode']
        for i in range(RETRY + 1):
            output = sshpass_output(namenode, 'jps')
            if 'NameNode' in output:
                break
            if i == RETRY:
                raise Exception('%s failed to start hadoop' % namenode)
            else:
                time.sleep(WAITTIME)
        
        datanodes = cluster['datanode']
        servers = _extract_addr(datanodes)
        for i in servers:
            output = sshpass_output(i, 'jps')
            if 'DataNode' not in output:
                raise Exception('%s failed to start hadoop' % i)

def _start_hadoop():
    if HDFS_CLUSTERS:
        name = get_name()
        stop_hdfs = os.path.join('/opt', name, 'sbin', 'stop-dfs.sh')
        start_hdfs = os.path.join('/opt', name, 'sbin', 'start-dfs.sh')
        for cluster in HDFS_CLUSTERS:
            namenode = cluster['namenode']
            sshpass(namenode, stop_hdfs)
            sshpass(namenode, start_hdfs)

def start_hadoop():
    _start_hadoop()
    _check_hadoop()
    
def _check_mongo():
    servers = load_conf(DB_SERVERS)
    for role in DB_SERVERS:
        for host in servers[role]:
            output = sshpass_output(host, 'lsof -i:%d' % MONGO_PORT)
            if str(MONGO_PORT) not in output:
                raise Exception('%s failed to start Mongo' % host)

def start_mongo():
    _check_mongo()

def start_all():
    start_mongo()
    start_hadoop()
    start_dpm()
