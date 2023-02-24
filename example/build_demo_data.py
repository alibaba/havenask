#!/bin/env python
import os
import sys
HERE = os.path.split(os.path.realpath(__file__))[0]

def executCmd(cmd):
    print cmd
    os.system(cmd)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'python build_demo_data.py installRoot case[optional]'
        print 'please specify install root'
        sys.exit(-1)
    
    installRoot = os.path.abspath(sys.argv[1])
    if len(sys.argv) == 3:
        case = sys.argv[2]
    else:
        case = "normal"
    caseRoot = os.path.abspath(os.path.join(HERE, "cases", case))
    workDir = os.path.join(caseRoot, './workdir')
    workDir = os.path.abspath(workDir)
    try:
        sys.path.append(os.path.abspath(
            os.path.join(HERE, "..", caseRoot)))
        from setup import build_prepare
        configDir, dataDir = build_prepare()   
    except Exception as e:
        print(e)
        print("failed to call build prepare function in %s/setup.py" % (caseRoot))
        exit(-1)                      
    dataDir = os.path.abspath(dataDir)
    configDir = os.path.abspath(configDir)

    print("check workDir %s"%(workDir))
    if os.path.exists(workDir):
        cmd = 'rm -rf ' + workDir
        print('check and rm', cmd)
        executCmd(cmd)
    cmd = 'mkdir ' + workDir
    executCmd(cmd)
    cmd = '%s/usr/local/bin/bs startjob -c %s -n in0 -j local -m full -d %s -w %s -i ./runtimedata -p 1 --documentformat=ha3' % (installRoot, configDir, dataDir, workDir)
    executCmd(cmd)
