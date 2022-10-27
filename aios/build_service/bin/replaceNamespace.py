#!/bin/env python

import sys

f = open(sys.argv[1], 'r')
lines = f.readlines()
f.close()

result = []
for l in lines:
    if 'BEGIN_BS_NAMESPACE' in l:
        ns = l[l.index('(') + 1:l.index(')')]
        l = 'namespace build_service {\nnamespace %s {\n' % ns
    if 'END_BS_NAMESPACE' in l:
        l = '};\n};\n'
    if 'USE_BS_NAMESPACE' in l:
        ns = l[l.index('(') + 1:l.index(')')]
        l = 'using namespace build_service::%s;\n' % ns
    l = l.replace('BS_NS(util)', 'util')
    l = l.replace('BS_NS(analyzer)', 'analyzer')
    if 'namespace.h' not in l:
        result.append(l)

f = open(sys.argv[1], 'w')
f.writelines(''.join(result))
f.close()
