#
# Copyright (C) 2016  Xu Tian <tianxu@iscas.ac.cn>
# Licensed under The MIT License (MIT)
# http://opensource.org/licenses/MIT
#

def _extract_addr(addr):
    addr_list = []
    if len(addr) <= 15:
        addr_list.append(addr)
    else:
        addr_start, addr_end = addr.split('-')
        head_start = '.'.join(addr_start.split('.')[:3])
        head_end = '.'.join(addr_end.split('.')[:3])
        if head_start != head_end:
            raise Exception('failed to load %s' % addr)
        start = int(addr_start.split('.')[-1])
        end = int(addr_end.split('.')[-1])
        for i in range (start, end + 1):
            addr = '%s.%d' % (head_start, i)
            addr_list.append(addr)
    return addr_list

def load_servers(name):
    servers = None
    addr_list = []
    exec('from conf.dpm.servers import %s as servers' % str(name))
    if not servers:
        raise Exception('failed to import servers')
    for addr in servers:
        addr_list += _extract_addr(addr)
    return addr_list

def load_conf(roles):
    res = {}
    for role in roles:
        addr_list = load_servers(role)
        if addr_list:
            res.update({role: addr_list})
    return res

def load_port(portname):
    port = None
    exec('from conf.dpm.servers import %s as port' % str(portname))
    if not port:
        raise Exception('failed to load port')
    return port
    