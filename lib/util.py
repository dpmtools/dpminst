#
# Copyright (C) 2016  Xu Tian <tianxu@iscas.ac.cn>
# Licensed under The MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os
from commands import getoutput
from conf.dpminst import PASSWORD, USERNAME

PRINT = True

def _get_cmd(host, cmd, background):
    if not background:
        return "sshpass -p %s ssh -o StrictHostKeyChecking=no %s@%s %s" % (PASSWORD, USERNAME, host, cmd)
    else:
        return "sshpass -p %s ssh -o StrictHostKeyChecking=no -f %s@%s %s" % (PASSWORD, USERNAME, host, cmd)

def sshpass(host, cmd, background=False):
    ssh_cmd = _get_cmd(host, cmd, background)
    if PRINT:
        print(ssh_cmd)
    os.system(ssh_cmd)

def sshpass_output(host, cmd, background=False):
    ssh_cmd = _get_cmd(host, cmd, background)
    if PRINT:
        print(ssh_cmd)
    return getoutput(ssh_cmd)

def scp_to(host, src, dst):
    os.system('sshpass -p %s scp -q %s %s@%s:%s' % (PASSWORD, src, USERNAME, host, dst))

def scp_from(host, src, dst):
    os.system('sshpass -p %s scp -q %s@%s:%s %s' % (PASSWORD, USERNAME, host, src, dst))