#!/bin/env python

import sys

f = open(sys.argv[1], 'r')
lines = f.readlines()
f.close()

result = []
classname = ''
for l in lines:
    if '/test.h' in l or 'Log.h' in l:
        continue
    if 'LOG_DECLARE' in l:
        result.pop()
        continue
    if 'LOG_SETUP' in l:
        result.pop()
        continue
    if 'BUILD_SERVICE_TESTBASE' in l:
        classname = l.split(':')[0].strip().split()[1]
        if '{' not in l:
            l = l[:-1] + ' {\n'
        result.append(l)
        continue
    if result and 'BUILD_SERVICE_TESTBASE' in result[-1] and l == '{':
        continue
    if 'void setUp();' in l:
        while 'BUILD_SERVICE_TESTBASE' not in result[-1]:
            result.pop()
        result.append('public:\n')
        result.append(l)
        continue
    if '%s::setUp() {' % classname in l:
        result.pop()
        result.pop()
        result.pop()
        result.pop()
        result.pop()
        result.pop()
    if 'setUp!' in l:
        continue
    if 'tearDown!' in l:
        continue
    if 'common_define.h' in l:
        continue
    result.append(l)

f = open(sys.argv[1], 'w')
f.writelines(''.join(result))
f.close()
