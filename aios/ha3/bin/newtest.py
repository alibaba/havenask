#!/bin/env python

import sys
import os
import re

template = '''#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/{%path%}{%classname%}.h"

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE({%ns%});

class {%classname%}Test : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP({%ns%}, {%classname%}Test);

void {%classname%}Test::setUp() {
}

void {%classname%}Test::tearDown() {
}

TEST_F({%classname%}Test, testSimple) {
}

END_HA3_NAMESPACE();

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

def createTestClassCPP(path, classFileName, classname, pathLevel, classSuffix):
    _, p = getNamespace(path, pathLevel)
    content= template
    ns = p.split("/")[0]
    content = content.replace('{%classname%}', classname)
    content = content.replace('{%path%}', p)
    content = content.replace('{%ns%}', ns)
    f = file(path + "/" + classFileName + classSuffix + ".cpp", "w")
    f.write(content)
    f.close()
    return True
        
def createTestClass(path, classFileName, pathLevel, classSuffix):
    classSuffixCpp = classSuffix + '.cpp'
    classname = classFileName

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
