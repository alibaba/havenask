#!/bin/python2.7
import sys
import re
import os
file_name = os.path.basename(sys.argv[1])
path_name = os.path.dirname(sys.argv[1])
classname = file_name.split('.')[0]
f = open(os.path.join(path_name, classname + '.h'), 'r')
hlines = f.readlines()
f.close()

start = False
classdeclare = []
headers = []
for idx, line in enumerate(hlines):
    if line.startswith('#include'):
        headers.append(line)
    if line.startswith('BEGIN_HA3_NAMESPACE'):
        start = True
        continue
    if line.startswith('END_HA3_NAMESPACE'):
        start = False
    if line.startswith('private:'):
        line = "protected:\n"
    if not re.match(".*void test.*\(\).*", line) and start:
        classdeclare.append(line)

f = open(os.path.join(path_name, classname + '.cpp'), 'r')
lines = f.readlines()
f.close()

caseNames = []
for l in hlines:
    prefix = 'CPPUNIT_TEST('
    if prefix in l:
        begin = l.find(prefix)
        if begin == -1:
            continue
        end = l.rfind(')')
        name = l[begin + len(prefix):end]
        caseNames.append(name)

newlines = []
for idx, line in enumerate(lines):
    if line.find(classname+'.h') != -1:
        newlines += headers;
        continue
    if line.startswith('BEGIN_HA3_NAMESPACE'):
        newlines.append(line)
        newlines += classdeclare
        continue
    lineStripped = line.strip()
    if lineStripped.startswith('void ') and classname + '::test' in lineStripped:
        prefix = '::'
        begin = line.find(prefix)
        end = line.rfind('(')
        name = line[begin+len(prefix):end]
        if name in caseNames:
            line = line.replace("void ", "TEST_F(")
            line = line.replace("::", ", ")
            line = line.replace("()", ")")
            newlines.append(line)
            continue
    if line.startswith('BUILD_SERVICE_UNIT_TEST_CASE'):
        continue
    newlines.append(line)

f = open(os.path.join(path_name, classname + '.cpp'), 'w')
f.writelines(newlines)
f.close()
    

