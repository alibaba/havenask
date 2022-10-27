#!/bin/env python

import sys
import os
import re

def resolveTypeName(type, name):
    for id in xrange(len(name)):
        if name[id] == '*':
            type += '*'
            name = name[1:]
        elif name[id] == '&':
            type += '&'
            name = name[1:]
        else:
            break
    return type, name

def getSig(full):
    full = full.split('=')[0]
    names = filter(None, full.split())
    type = ' '.join(names[:-1])
    name = names[-1]
    return resolveTypeName(type, name)[0]

def getClassFunSigs(path, classname):
    f = open(path, 'r')
    c = f.readlines()
    f.close()
    sigs = []
    begin = False
    visibility = ''
    isConst = False
    func = ''
    for idx, line in enumerate(c):
        if line.find('class ' + classname) != -1:
            begin = True
        if not begin:
            continue
        if line.find('public:') != -1:
            visibility = 'public:'
        if line.find('protected:') != -1:
            visibility = 'protected:'
        if line.find('private:') != -1:
            visibility = 'private:'
        if line.find('const') != -1:
            isConst = True
        if begin and line[:2] == '};':
            return sigs
        if func or (line.find('virtual') != -1 and line.find('~' + classname) == -1):
            func += line
            if func.find(';') != -1:
                fun = func[func.find('virtual') + len('virtual') + 1:]
                argBegin = fun.find('(')
                argEnd = fun.rfind(')')
                ss = filter(None, fun[:argBegin].split())
                reType, name = resolveTypeName(ss[0], ss[1])
                args = map(getSig, filter(None, fun[argBegin + 1:argEnd].split(',')))
                sigs.append((name, reType, args, visibility, isConst))
                func = ''
    return sigs

def createMockClassH(headerPath):
    absPath = os.path.abspath(headerPath)
    path, file = os.path.split(absPath)
    _, ns = os.path.split(path)
    classname = file.split('.')[0]
    mockClassName = 'Mock' + classname
    sigs = getClassFunSigs(headerPath, classname);

    context = ''
    context += '#ifndef ISEARCH_BS_' + mockClassName.upper() + '_H' + '\n'
    context += '#define ISEARCH_BS_' + mockClassName.upper() + '_H' + '\n'
    context += '\n'
    context += '#include \"build_service/test/unittest.h\"' + '\n'
    context += '#include \"build_service/test/test.h\"' + '\n'
    context += '#include \"build_service/util/Log.h\"' + '\n'
    context += '#include \"build_service/' + ns + '/' + classname + '.h\"' + '\n'
    context += '\n'
    context += 'namespace build_service {\n'
    context += 'namespace ' + ns + ' {\n'
    context += '\n'
    context += 'class ' + mockClassName + ' : public ' + classname + '\n'
    context += '{' + '\n'
    context += 'public:' + '\n'
    context += '    ' + mockClassName + '() {}' + '\n'
    context += '    ~' + mockClassName + '() {}' + '\n'
    context += '\n'
    visibility = ''
    for sig in sigs:
        if sig[3] != visibility:
            visibility = sig[3]
            context += visibility + '\n'
        if sig[4] != True:
            context += '    MOCK_METHOD%d(%s, %s(%s));\n' % (len(sig[2]), sig[0], sig[1], ', '.join(sig[2]))
        else:
            context += '    MOCK_CONST_METHOD%d(%s, %s(%s));\n' % (len(sig[2]), sig[0], sig[1], ', '.join(sig[2]))
    context += '};' + '\n'
    context += '\n'
    context += 'typedef ::testing::StrictMock<' + mockClassName + '> Strict' + mockClassName + ';\n'
    context += 'typedef ::testing::NiceMock<' + mockClassName + '> Nice' + mockClassName + ';\n'
    context += '\n'
    context += '}\n'
    context += '}\n'
    context += '\n'
    context += '#endif //ISEARCH_BS_' + mockClassName.upper() + '_H' + '\n'

    f = open(os.getcwd() + '/' + mockClassName + '.h', 'w')
    f.write(context)
    f.close()
    return True

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print >> sys.stderr, 'Usage:', sys.argv[0], '<headerpath>'
        sys.exit(1)

    createMockClassH(sys.argv[1])
