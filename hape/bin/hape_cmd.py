# -*- coding: utf-8 -*-

import os
import sys
hape_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path = [hape_path, os.path.join(hape_path, "bin"), os.path.join(hape_path, "hape_depends")] + sys.path

from hape.commands import *
from hape.domains.admin.daemon.daemon_manager import AdminDaemonManager

usage = '''
usage:
python bin/hape_cmd.py [subcommands] [arguments]

hape subcommands:
    init                init biz config and hape config
    check               check problem in havenask domain
    start               start havenask domain
    stop                stop havenask domain
    remove              remove havenask domain
    gs                  get status of havenask domain
    gt                  get target of havenask domain
    upf                 update full index of havenask searchers
    upc                 update biz config of havenask workers
    build_full_index    build full index
    restart_daemon      restart domain daemon 
    kill_daemon         kill domain daemon

see more detail of subcommands, do:
    python bin/hape_cmd.py [subcommands]
'''

def __usage(bin):
    return  usage % {'prog' : bin}


def run(argv):
        
    if len(argv) < 2:
        print(__usage(argv[0]))
        return True

    cmd_str = argv[1]
    if cmd_str == 'init':
        InitCommand().init(argv[2:])
    elif cmd_str == 'start':
        StartCommand().start(argv[2:])
    elif cmd_str == "stop":
        StopCommand().stop(argv[2:])
    elif cmd_str == "remove":
        RemoveCommand().remove(argv[2:])
    elif cmd_str == "build_full_index":
        StartCommand().build_full_index(argv[2:])
    elif cmd_str == "dp":
        DpCommand().dp(argv[2:])
    elif cmd_str == "upf":
        UpfCommand().upf(argv[2:])
    elif cmd_str == "upc":
        UpcCommand().upc(argv[2:])
    elif cmd_str == "gs":
        StatusCommand().get_status(argv[2:])
    elif cmd_str == "gt":
        TargetCommand().get_target(argv[2:])
    elif cmd_str == "check":
        CheckCommand().check(argv[2:])
    elif cmd_str == "restart_daemon":
        DaemonCommand().restart_daemon(argv[2:])
    elif cmd_str == "kill_daemon":
        DaemonCommand().kill_daemon(argv[2:])
    else:
        raise ValueError("Unknown command [{}]".format(cmd_str))
    

if __name__ == "__main__":
    run(sys.argv)