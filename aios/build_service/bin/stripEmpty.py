#!/bin/env python

import sys

f = open(sys.argv[1], 'r')
lines = f.readlines()
f.close()

result = []
for l in lines:
    l = l.rstrip()
    result.append(l)

while not result[-1]:
    result.pop()
result.append('')

f = open(sys.argv[1], 'w')
f.writelines('\n'.join(result))
f.close()
