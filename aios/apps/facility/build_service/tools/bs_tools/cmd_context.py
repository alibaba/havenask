# -*- coding: utf-8 -*-

usage = '''
usage:
%(prog)s [options]

service commands:
    startservice[sts]               start service
    stopservice[sps]                stop service
    getstatus[gs]                   get service status
    startbuild[stb]                 start build index
    stopbuild[spb]                  stop build index
    createversion[cv]               create version
    switchbuild[sb]                 switch build
    updateconfig[upc]               update config

config commands:
    deploy[dp]                      deploy configurations
    download[dl]                    download configurations
    validateconfig[vc]              validate configurations

package commands:
    addpackage[ap]                  add job package
    removepackage[rp]               remove job package
    listpackage[lp]                 list job package

job commands:
    startjob[stj]                   start job
    stopjob[spj]                    stop job
    getjobstatus[gjs]               get job status

others:
    --version[-v]                   get tools version

see more details, do:
    %(prog)s -h|--help
'''

def __usage(bin):
    return  usage % {'prog' : bin}

def run(argv):
    cmd = None
    debugMode = '--debug' in argv
    argv = filter(lambda x : x != '--debug', argv)

    if len(argv) < 2:
        print __usage(argv[0])
        return True

    cmdStr = argv[1]
    if cmdStr in ['startservice', 'sts']:
        import app_cmd
        cmd = app_cmd.StartServiceCmd()
    elif cmdStr in ['stopservice', 'sps']:
        import app_cmd
        cmd = app_cmd.StopServiceCmd()
    elif cmdStr in ['updateadmin', 'upa']:
        import app_cmd
        cmd = app_cmd.UpdateAdminCmd()
    elif cmdStr in ['getappstatus', 'gas']:
        import app_cmd
        cmd = app_cmd.GetAppStatusCmd()
    elif cmdStr in ['getstatus', 'gs']:
        import status_cmd
        cmd = status_cmd.GetStatusCmd()
    elif cmdStr in ['startbuild', 'stb']:
        import build_cmd
        cmd = build_cmd.StartBuildCmd()
    elif cmdStr in ['stopbuild', 'spb']:
        import build_cmd
        cmd = build_cmd.StopBuildCmd()
    elif cmdStr in ['updateconfig', 'upc']:
        import build_cmd
        cmd = build_cmd.UpdateConfigCmd()
    elif cmdStr in ['stepbuild', 'sbd']:
        import build_cmd
        cmd = build_cmd.StepBuildCmd()
    elif cmdStr in ['createversion', 'cv']:
        import build_cmd
        cmd = build_cmd.CreateVersionCmd()
    elif cmdStr in ['switchbuild', 'sb']:
        import build_cmd
        cmd = build_cmd.SwitchCmd()
    elif cmdStr in ['deploy', 'dp']:
        import config_cmd
        cmd = config_cmd.DeployCmd()
    elif cmdStr in ['download', 'dl']:
        import config_cmd
        cmd = config_cmd.DownloadCmd()
    elif cmdStr in ['validateconfig', 'vc']:
        import config_cmd
        cmd = config_cmd.ValidateConfigCmd()
    elif cmdStr in ['addpackage', 'ap']:
        import package_cmd
        cmd = package_cmd.AddPackageCmd()
    elif cmdStr in ['removepackage', 'rp']:
        import package_cmd
        cmd = package_cmd.RemovePackageCmd()
    elif cmdStr in ['listpackage', 'lp']:
        import package_cmd
        cmd = package_cmd.ListPackageCmd()
    elif cmdStr in ['startjob', 'stj']:
        import job_cmd
        cmd = job_cmd.StartJobCmd()
    elif cmdStr in ['stopjob', 'spj']:
        import job_cmd
        cmd = job_cmd.StopJobCmd()
    elif cmdStr in ['getjobstatus', 'gjs']:
        import job_cmd
        cmd = job_cmd.GetJobStatusCmd()
    elif cmdStr in ['--version', '-v']:
        import version
        print version.bs_tools_version
        return True
    elif cmdStr in ['--help', '-h']:
        print __usage(argv[0])
        return True
    else:
        print __usage(argv[0])
        return False

    try:
        cmd.parseParams(argv[2:])
        cmd.run()
    except Exception, e:
        print str(e)
        if debugMode:
            import traceback
            traceback.print_exc()
        return False

    return True
