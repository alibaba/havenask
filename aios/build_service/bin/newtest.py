#!/bin/env python

import sys
import os
import re

template = '''#include "build_service/test/unittest.h"
#include "build_service/{%path%}{%classname%}.h"

using namespace std;
using namespace testing;

namespace build_service {
namespace {%namespace%} {

class {%classname%}Test : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void {%classname%}Test::setUp() {
}

void {%classname%}Test::tearDown() {
}

TEST_F({%classname%}Test, testSimple) {
    ASSERT_TRUE(false) << "!not implemented!";
}

}
}
'''

def doGetNamespace(path):
    ls = path.split("/")
    n = len(ls)
    while n >= 1 and len(ls[n-1]) == 0:
        n = n - 1
    if n < 2:
        return None, None
    if ls[n-1] != "test":
        return None, None
    n = n - 1
    while n >= 1 and len(ls[n-1]) == 0:
        n = n - 1
    if n < 1:
        return None, None
    i = 0
    ret = ''
    for p in ls:
        if i < n - 1:
            ret = os.path.join(ret, p)
            i = i + 1
        ret = os.path.join(ret, "test")
    return ls[n - 1], ret

def getNamespace(path, pathLevel):
    i = pathLevel
    p = path
    ret = ''
    while i > 0 and p != None:
        r, p = doGetNamespace(p)
        i = i - 1
        if r == None:
            break
        ret = os.path.join(r, ret)

    return r, ret

def getClassName(path):
    LLL = path.split('_')
    for i in range(len(LLL)): LLL[i] = LLL[i][0].upper() + LLL[i][1:]
    return ''.join(LLL)

def createTestClassCPP(path, classFileName, classname, pathLevel, classSuffix):
    ns, p = getNamespace(path, pathLevel)
    if not ns:
        return False
    content= template
    content = content.replace('{%namespace%}', ns)
    content = content.replace('{%classname%}', classname)
    content = content.replace('{%path%}', p)
    f = file(path + "/" + classFileName + classSuffix + ".cpp", "w")
    f.write(content)
    f.close()
    return True
        
def createTestClass(path, classFileName, pathLevel, classSuffix):
    classSuffixCpp = classSuffix + '.cpp'
    classname = getClassName(classFileName)

    sourceFilePath = path + "/" + classFileName + classSuffixCpp
    if os.path.exists(sourceFilePath):
        print "source file " + sourceFilePath + " exists!"
    else:
        if createTestClassCPP(path, classFileName, classname, pathLevel, classSuffix):
            print "create source file " + sourceFilePath + " succeed."
        else:
            print "create source file " + sourceFilePath + " failed."
            return False
    return True

def insertSConscript(scon_name, name, classSuffix):
    hasFindSource = False
    os.system("mv " + scon_name + " " + scon_name + ".bak")
    TIP = "_test_sources\s*=\s*\["
    fin = file(scon_name + ".bak", "r");
    fout = file(scon_name, "w");
    while True:
        l = fin.readline()
        if not l:
            break;
        if not hasFindSource:
            if re.search(TIP, l):
                hasFindSource = True
        elif l.find(']') != -1:
            l = "    \'" + name + classSuffix + ".cpp\',\n" + l
            hasFindSource = False
        fout.write(l)
    fin.close()
    fout.close()
    print name + " has been inserted into " + scon_name
    os.system("rm " + scon_name + ".bak")

if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 4:
        print >> sys.stderr, "Usage:", sys.argv[0], "<classname> [path level] [class suffix]"
        sys.exit(1)
    path = os.getcwd()
    pathLevel = 1
    if len(sys.argv) >= 3:
        pathLevel = int(sys.argv[2])

    classSuffix = 'Test'
    if len(sys.argv) == 4:
        classSuffix = sys.argv[3]

    if createTestClass(path, sys.argv[1], pathLevel, classSuffix):
        insertSConscript(path + "/SConscript", sys.argv[1], classSuffix)
