#!/bin/env python
import sys
import os
import re

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
    while n >= 1 and ls[n] <> 'build_service':
        n = n - 1
    n = n + 1
    path = ""
    if n <= dirCount:
        path = ls[n]
    while n < dirCount :
        path =path + "/"+ls[n+1]
        n = n+1
    return path


def createClassH(path, classname):
    ns = getNamespace(path)
    if not ns:
        return False
    context = ""
    context += "#ifndef ISEARCH_BS_" + classname.upper() + "_H" + "\n"
    context += "#define ISEARCH_BS_" + classname.upper() + "_H" + "\n"
    context += "" + "\n"
    context += "#include \"build_service/common_define.h\"" + "\n"
    context += "#include \"build_service/util/Log.h\"" + "\n"
    context += "" + "\n"
    context += 'namespace build_service {\n'
    context += 'namespace ' + ns + ' {\n'
    context += "" + "\n"
    context += "class " + classname + "\n"
    context += "{" + "\n"
    context += "public:" + "\n"
    context += "    " + classname + "();" + "\n"
    context += "    ~" + classname + "();" + "\n"
    context += "private:" + "\n"
    context += "    " + classname + "(const " + classname + " &);" + "\n"
    context += "    " + classname + "& operator=(const " + classname + " &);" + "\n"
    context += "public:" + "\n"
    context += "" + "\n"
    context += "private:" + "\n"
    context += "    BS_LOG_DECLARE();" + "\n"
    context += "};" + "\n"
    context += '\n'
    context += "BS_TYPEDEF_PTR(" + classname + ");\n"
    context += '\n'
    context += '}\n'
    context += '}\n'
    context += '\n'
    context += "#endif //ISEARCH_BS_" + classname.upper() + "_H" + "\n"
    f = file(path + "/" + classname + ".h", "w")
    f.write(context)
    f.close()
    return True

def createClassCPP(path, classname):
    ns = getNamespace(path)
    hp = getHeaderPath(path)
    if not ns:
        return False
    context = ""
    if (not re.match(".*/test", path)):
        context += "#include \"build_service/" + hp + "/" + classname + ".h\"" + "\n"
    else:
        context += "#include \"build_service/" + hp + "/test/" + classname + ".h\"" + "\n"
    context += "" + "\n"
    context += "using namespace std;" + "\n"
    context += "\n"
    context += 'namespace build_service {\n'
    context += 'namespace ' + ns + ' {\n'
    context += "BS_LOG_SETUP(" + ns + ", " + classname + ");" + "\n"
    context += "" + "\n"
    context += classname + "::" + classname + "() {" + "\n"
    context += "}" + "\n"
    context += "" + "\n"
    context += classname + "::~" + classname + "() {" + "\n"
    context += '}\n'
    context += '\n'
    context += '}\n'
    context += '}\n'
    f = file(path + "/" + classname + ".cpp", "w")
    f.write(context)
    f.close()
    return True

def createClass(path, classname):
    ret = 0
    if os.path.exists(path + "/" + classname + ".h"):
        print "header file " + path + "/" + classname + ".h exists!"
    else:
        if createClassH(path, classname):
            print "create header file " + path + "/" + classname + ".h succeed."
            ret = ret + 1
        else:
            print "create header file " + path + "/" + classname + ".h failed."
    if os.path.exists(path + "/" + classname + ".cpp"):
        print "source file " + path + "/" + classname + ".cpp exists!"
    else:
        if createClassCPP(path, classname):
            print "create source file " + path + "/" + classname + ".cpp succeed."
            ret = ret + 1
        else:
            print "create source file " + path + "/" + classname + ".cpp failed."
    return ret

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

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage:", sys.argv[0], "<classname>"
        sys.exit(1)
    path = os.getcwd()
    if createClass(path, sys.argv[1]) == 2:
        insertSConscript(path + "/SConscript", sys.argv[1])
