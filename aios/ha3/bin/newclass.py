#!/bin/env python

import sys
import os
import re
import argparse

def getNamespace(path):
    ls = path.split("/")
    n = len(ls)
    while n >= 1 and len(ls[n-1]) == 0:
        n = n - 1
    if n < 1:
        return None
    if ls[n-1] == 'test':
        return ls[n-2]
    else:
        return ls[n-1]

def getHeaderPath(path):
    ls = path.split("/")
    n = len(ls)
    while n >= 1 and len(ls[n-1]) == 0:
        n = n - 1
    if n < 1:
        return None
    n = n-1;
    if ls[n] == 'test':
        n = n-1

    dirCount = n;
    while n >= 1 and ls[n] <> 'ha3':
        n = n - 1
    n = n + 1
    path = ""
    if n <= dirCount:
        path = ls[n]
    while n < dirCount :
        path =path + "/"+ls[n+1]
        n = n+1
    return path


def createClassH(path, classname, ns):
    ns = ns or getNamespace(path)
    tpl = """#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE({ns});

class {className}
{{
public:
    {className}();
    ~{className}();
    {className}(const {className} &) = delete;
    {className}& operator=(const {className} &) = delete;
private:
    HA3_LOG_DECLARE();
}};

HA3_TYPEDEF_PTR({className});
END_HA3_NAMESPACE({ns});
"""
    code = tpl.format(className=classname, ns=ns)
    f = file(path + "/" + classname + ".h", "w")
    f.write(code)
    f.close()
    return True

def createClassCPP(path, classname, ns):
    ns = ns or getNamespace(path)
    hp = getHeaderPath(path)
    if not ns:
        return False
    context = ""
    if (not re.match(".*/test", path)):
        context = context + "#include <ha3/" + hp + "/" + classname + ".h>" + "\n"
    else:
        context = context + "#include <ha3/" + hp + "/test/" + classname + ".h>" + "\n"
    context = context + "" + "\n"
    context = context + "using namespace std;" + "\n"
    context = context + "" + "\n"
    context = context + "BEGIN_HA3_NAMESPACE(" + ns + ");" + "\n"
    context = context + "HA3_LOG_SETUP(" + ns + ", " + classname + ");" + "\n"
    context = context + "" + "\n"
    context = context + classname + "::" + classname + "() { " + "\n"
    context = context + "}" + "\n"
    context = context + "" + "\n"
    context = context + classname + "::~" + classname + "() { " + "\n"
    context = context + "}" + "\n"
    context = context + "" + "\n"
    context = context + "END_HA3_NAMESPACE(" + ns + ");" + "\n"
    context = context + "" + "\n"
    f = file(path + "/" + classname + ".cpp", "w")
    f.write(context)
    f.close()
    return True

def createClass(path, classname, ns):
    ret = 0
    if os.path.exists(path + "/" + classname + ".h"):
        print "header file " + path + "/" + classname + ".h exists!"
    else:
        if createClassH(path, classname, ns):
            print "create header file " + path + "/" + classname + ".h succeed."
            ret = ret + 1
        else:
            print "create header file " + path + "/" + classname + ".h failed."
    if os.path.exists(path + "/" + classname + ".cpp"):
        print "source file " + path + "/" + classname + ".cpp exists!"
    else:
        if createClassCPP(path, classname, ns):
            print "create source file " + path + "/" + classname + ".cpp succeed."
            ret = ret + 1
        else:
            print "create source file " + path + "/" + classname + ".cpp failed."
    return ret

def insertMakefile(makefile, name):
    TIP = "source_list"
    TIPLEN = len(TIP)
    TIP2 = "EXTRA_DIST"
    TIP2LEN = len(TIP2)
    fin = file(makefile, "r")
    fbak = file(makefile + ".bak", "w")
    context = fin.read()
    fin.close()
    fbak.write(context)
    fbak.close()
    fout = file(makefile, "w")
    fin = file(makefile + ".bak", "r")
    while True:
        l = fin.readline()
        if not l:
            break
        if len(l) > TIPLEN and l[:TIPLEN] == TIP:
            l = l + "    " + name + ".cpp\\\n"
#      n = l.find("=")
#      if n != -1:
#    l = l[:n+1] + " " + name + ".cpp " + l[n+1:]
            if len(l) > TIP2LEN and l[:TIP2LEN] == TIP2:
                l = l + "    " + name + ".h\\\n"
#      n = l.find("=")
#      if n != -1:
#    l = l[:n+1] + " " + name + ".h " + l[n+1:]
                fout.write(l)
                fin.close()
                fout.close()
                print name + " has been inserted into " + makefile


def insertSConscript(scon_name, name):
    hasFindSource = False
    hasWritten = False
    os.system("mv " + scon_name + " " + scon_name + ".bak")
    TIP = ur"_sources\s*=\s*\["
    fin = file(scon_name + ".bak", "r");
    fout = file(scon_name, "w");
    while True:
        l = fin.readline()
        if not l:
            break;
        if not hasFindSource and not hasWritten:
            if re.search(TIP, l):
                hasFindSource = True
        elif not hasWritten and l.find(']') != -1:
            l = "    \'" + name + ".cpp\',\n" + l
            hasFindSource = False
            hasWritten = True
        fout.write(l)
    fin.close()
    fout.close()
    print name + " has been inserted into " + scon_name
    os.system("rm " + scon_name + ".bak")
def main():
    parser = argparse.ArgumentParser(
        description='ha3 newclass util',
        usage='%(prog)s newclass')
    parser.add_argument('classname')
    parser.add_argument('--ns', dest='namespace', default=None)
    args = parser.parse_args()
    path = os.getcwd()
    if createClass(path, args.classname, args.namespace) == 2:
        insertSConscript(path + "/SConscript", sys.argv[1])
if __name__ == "__main__":
    main()
#           insertMakefile(path + "/Makefile.am", sys.argv[1])
