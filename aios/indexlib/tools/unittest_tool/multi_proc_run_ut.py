import os, sys, subprocess, datetime
from sys import argv
from time import sleep

exePath = ''
procNum = 0
runNum = 0

def howto(argv):
    print 'usage: python', argv[0], ' exe_path procNum runNum'

try:
    exePath = argv[1]
    procNum = int(argv[2])
    runNum = int(argv[3])
except:
    howto(argv)
    sys.exit(1)

if not os.path.isfile(exePath):
    print exePath, 'not exists.'
    sys.exit(1)

exeName = os.path.basename(exePath)
exeDirs = ['mpru_' + str(i) for i in range(procNum)]
exePaths = [s + '/' + exeName for s in exeDirs]
runShellPaths = [s + '/run.py' for s in exeDirs]

# print exeDirs
# print exePaths
# print runShellPaths

runpy = '''#!/usr/bin/python
import os, sys
for i in range(%d):
    stdout = str(i) + '.stdout'
    stderr = str(i) + '.stderr'
    cmd = 'ulimit -c unlimited; ./%s > ' + stdout + ' 2> ' + stderr
    ret = os.system(cmd)
    if ret != 0:
        print 'found Error'
        sys.exit(-1)

print 'done'
''' % (runNum, exeName)

procList = []
for i in range(procNum):
    if not os.path.isdir(exeDirs[i]):
        os.mkdir(exeDirs[i])
        os.system('cp *.json %s/' % exeDirs[i])
    if not os.path.isfile(exePaths[i]):
        os.system('cp %s %s' % (exePath, exePaths[i]))
    open(runShellPaths[i], 'w').write(runpy)
    os.system('chmod +x ' + runShellPaths[i])
    cmd = 'cd %s; python %s' % (os.path.dirname(runShellPaths[i]), os.path.basename(runShellPaths[i]))
    # print cmd
    procList.append((subprocess.Popen(cmd, shell=True), i))

def check(procList):
    aliveCC = 0
    for (p,i) in procList:
        ret = p.poll()
        if ret == None:
            aliveCC += 1
            print 'proc ', i, 'still running'
        else:
            print 'proc ', i, 'return', ret
    return aliveCC

while 1:
    aliveCC = check(procList)
    if aliveCC > 0:
        print '========================================', datetime.datetime.now()
    else: 
        break
    sleep(1)

print 'done'

