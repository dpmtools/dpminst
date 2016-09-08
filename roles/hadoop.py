#
# Copyright (C) 2016  Xu Tian <tianxu@iscas.ac.cn>
# Licensed under The MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os
import sys
import shutil
import commands
from threading import Thread
from tempfile import mktemp
from lib.loadconf import _extract_addr
from lib.util import sshpass, sshpass_output, scp_to, scp_from
from conf.hadoop.config import HDFS_CLUSTERS, HADOOP_URL, HADOOP_TEMP, CLUSTER_NAME, HADOOP_PORT, HDFS_REPLICATION

def _get_filename():
    return HADOOP_URL.split('/')[-1]

def get_name():
    filename = _get_filename()
    pos= filename.index('tar')
    return filename[:pos - 1]

def download_hadoop():
    if os.path.exists(HADOOP_TEMP):
        os.makedirs(HADOOP_TEMP, 0o755)
    os.system('wget %s -P %s' % (HADOOP_URL, HADOOP_TEMP))

def _login_ssh(namenode, datanodes):
    if datanodes:
        tmp = mktemp()
        sshpass(namenode, '\'if [ ! -e ~/.ssh/id_rsa.pub ]; then ssh-keygen -t rsa -P \"\" -f ~/.ssh/id_rsa; fi\'' )
        scp_from(namenode, '~/.ssh/id_rsa.pub', tmp)
        try:
            for server in datanodes:
                scp_to(server, tmp, tmp)
                sshpass(server, '\'cat %s >> %s\''  % (tmp, '~/.ssh/authorized_keys'))
                sshpass(server, 'rm %s' % tmp)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

def _gen_slaves(path, datanodes):
    with open(path, 'w') as f:
        f.writelines( map(lambda i:i + '\n', datanodes))

def _gen_hdfs(path, datanodes):
    num = 0
    head = ''
    content = []
    name = get_name()
    with open(path) as f:
        lines = f.readlines()
        for i in range(len(lines)):
            if '<value>file' not in lines[i]:
                if 'dfs.replication' in lines[i]:
                    num = i
                    head = lines[i].split('<name>')[0]
                content.append(lines[i])
            else:
                start, end = lines[i].split(':/')
                res = end.split('/')
                for j in res:
                    if 'hadoop' in j:
                        pos = res.index(j)
                        del res[pos]
                        res.insert(pos, name)
                        content.append('%s:/%s' % (start, '/'.join(res)))
    
    del content[num + 1]
    content.insert(num + 1, '%s%s%s%s' % (head, '<value>', str(HDFS_REPLICATION), '</value>\n'))
    
    with open(path, 'w+') as f:
        f.writelines(content)

def _gen_core(path, namenode):
    content = []
    with open(path) as f:
        lines = f.readlines()
        for i in range(len(lines)):
            if '<value>hdfs' not in lines[i]:
                content.append(lines[i])
            else:
                start, _ = lines[i].split('://')
                content.append('%s://%s:%d%s' % (start, namenode, HADOOP_PORT, '</value>\n'))
    with open(path, 'w+') as f:
        f.writelines(content)

def _gen_env(path, server):
    output = sshpass_output(server, 'ls %s' % '/usr/lib/jvm')
    content = output.split('\n')
    for item in content:
        if '7-openjdk' in item:
            env = item
            break
    buf = []
    with open(path) as f:
        lines = f.readlines()
        for i in range(len(lines)):
            if 'export JAVA_HOME' not in lines[i]:
                buf.append(lines[i])
            else:
                head, _ = lines[i].split('=')
                buf.append('%s=%s\n' % (head, os.path.join('/usr/lib/jvm', env)))
    with open(path, 'w+') as f:
        f.writelines(buf)

def _copy_conf(name, path):
    filename = commands.getoutput('readlink -f %s' % sys.argv[0])
    current_dir = os.path.dirname(filename)
    home = os.path.dirname(current_dir)
    src = os.path.join(home, 'conf', 'hadoop', name)
    shutil.copy(src, path)

def _node_name(cluster_name, index):
    return '%sn%d' % (cluster_name, int(index))

def _check_hosts(server, buf):
    path = mktemp()
    scp_from(server, '/etc/hosts', path)
    try:
        with open(path) as f:
            lines = f.readlines()
            for i in range(len(buf)):
                res = buf[i].split(' ')
                host = res[0]
                name = res[-1].strip()
                for j in range(len(lines)):
                    if name in lines[j]:
                        raise Exception('%s have a invalid name %s, it has been named' % (host, name))
    finally:
        os.remove(path)

def _update_hosts(cluster_name, servers):
    buf = []
    hosts = []
    for i in range(len(servers)):
        hostname = _node_name(cluster_name, i)
        hosts.append(hostname)
        buf.append('%s    %s\n' % (servers[i], hostname))
        sshpass(servers[i], "\'echo \"%s\">/etc/hostname;hostname %s\'" % (hostname, hostname))
    
    path = mktemp()
    with open(path, 'w') as f:
        f.writelines(buf)
    try:
        for i in range(len(servers)):
            _check_hosts(servers[i], buf)
            scp_to(servers[i], path, path)
            sshpass(servers[i], "\'cat %s>>/etc/hosts;rm %s\'" % (path, path))
            sshpass(servers[i], "\'echo  \"StrictHostKeyChecking no\">>~/.ssh/config\'")
            sshpass(servers[i], "\'echo  \"UserKnownHostsFile /dev/null\">>~/.ssh/config\'")
            sshpass(servers[i], "service ssh restart")
        
        return hosts
    finally:
        if os.path.exists(path):
            os.remove(path)

def _config_server(server, hdfsfile, path_slaves, path_core, path_hdfs):
    name = get_name()
    path_tmp = '/opt/.hdfsfile'
    sshpass(server, 'apt-get -y install %s' % 'openjdk-7-jdk')
    scp_to(server, hdfsfile, path_tmp)
    sshpass(server, 'rm -rf %s' % os.path.join('/opt', name))
    sshpass(server, 'tar zxf %s --directory %s' % (path_tmp, '/opt'))
    sshpass(server, 'rm %s' % path_tmp)
    
    src = os.path.join('/opt', name)
    dir_data = os.path.join(src, 'hdfs', 'data')
    dir_name = os.path.join(src, 'hdfs', 'name')
    sshpass(server, 'mkdir -p %s %s' % (dir_data, dir_name))
    
    path_env = mktemp()
    try:
        _copy_conf('hadoop-env.sh', path_env)
        _gen_env(path_env, server)
        dst = os.path.join('/opt', name, 'etc', 'hadoop', 'hadoop-env.sh')
        scp_to(server, path_env, dst)
    finally:
        os.remove(path_env)
    
    dst = os.path.join('/opt', name, 'etc', 'hadoop', 'slaves')
    scp_to(server, path_slaves, dst)
    
    dst = os.path.join('/opt', name, 'etc', 'hadoop', 'core-site.xml')
    scp_to(server, path_core, dst)
    
    dst = os.path.join('/opt', name, 'etc', 'hadoop', 'hdfs-site.xml')
    scp_to(server, path_hdfs, dst)

def _config_cluster(cluster, cluster_id, hdfsfile):
    truncate = True
    name = get_name()
    namenode = cluster['namenode']
    datanodes = cluster['datanode']
    servers = _extract_addr(datanodes)
    cluster_name = CLUSTER_NAME + str(cluster_id)
    
    if HDFS_REPLICATION > len(servers):
        raise Exception('failed to config replication')
    
    if namenode not in servers:
        servers.insert(0, namenode)
    else:
        truncate = False
    
    _login_ssh(namenode, servers)
    hosts = _update_hosts(cluster_name, servers)
    if truncate:
        slaves = hosts[1:]
    else:
        slaves = hosts
    
    path_slaves = mktemp()
    _gen_slaves(path_slaves, slaves)
    
    path_core = mktemp()
    _copy_conf('core-site.xml', path_core)
    _gen_core(path_core, hosts[0])
    
    path_hdfs = mktemp()
    _copy_conf('hdfs-site.xml', path_hdfs)
    _gen_hdfs(path_hdfs, slaves)
    
    threads = []
    for server in servers:
        th = Thread(target=_config_server, args=(server, hdfsfile, path_slaves, path_core, path_hdfs))
        threads.append(th)
        th.start()
    
    for th in threads:
        th.join()
    
    target = os.path.join('/opt', name, 'bin', 'hadoop')
    sshpass(namenode, '%s namenode -format' % target)
    os.remove(path_slaves)
    os.remove(path_core)
    os.remove(path_hdfs)

def install_hadoop():
    download_hadoop()
    current = {}
    
    if HDFS_CLUSTERS:
        for cluster in HDFS_CLUSTERS:
            namenode = cluster['namenode']
            datanodes = cluster['datanode']
            servers = _extract_addr(datanodes)
            for i in current:
                if namenode in current[i]:
                    raise Exception('Error: invalid namenode %s' % str(namenode))
                for j in servers:
                    if j in current[i]:
                        raise Exception('Error: invalid datanode %s' % str(j))
            current[namenode] = servers
                
        filename = _get_filename()
        hdfsfile = os.path.join(HADOOP_TEMP, filename)
        
        try:
            cluster_id = 0
            threads = []
            for cluster in HDFS_CLUSTERS:
                th = Thread(target=_config_cluster, args=(cluster, cluster_id, hdfsfile))
                threads.append(th)
                th.start()
                cluster_id += 1
        
            for th in threads:
                th.join()
        finally:
            os.remove(hdfsfile)
    
    shutil.rmtree(HADOOP_TEMP)
