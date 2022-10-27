#!/bin/env python

import sys
import os
import re

def getNamespace(path):
	if 'indexlib/' not in path:
		return "NONE", ''
	LLL = path[path.rfind('indexlib/') + len('indexlib/') : ].split("/")
	if not LLL or not LLL[0]: 
		return "NONE", ''

        i = 0
        n = len(LLL)
        ret = ''
        for p in LLL:
		if i < n - 1:
			ret = os.path.join(ret, p)
			i = i + 1
	return LLL[0], ret

	
def getClassName(path):
	LLL = path.split('_')
	for i in range(len(LLL)): LLL[i] = LLL[i][0].upper() + LLL[i][1:]
	return ''.join(LLL)

def createTestClassH(path, classFileName, classname, classSuffix):
	ns, p = getNamespace(path)
	if not ns:
		return False
	context = ""
	context = context + "#ifndef __INDEXLIB_" + classname.upper() + "TEST_H" + "\n"
	context = context + "#define __INDEXLIB_" + classname.upper() + "TEST_H" + "\n"
	context = context + "" + "\n"
	context = context + "#include \"indexlib/common_define.h\"" + "\n"
	context = context + "#include \"indexlib/test/test.h\"" + "\n"
	context = context + "#include \"indexlib/test/unittest.h\"" + "\n"
	context = context + "#include \"indexlib/" + p + "/" + classFileName + ".h\"" + "\n"
	context = context + "" + "\n"
	context = context + "IE_NAMESPACE_BEGIN(" + ns + ");" + "\n"
	context = context + "" + "\n"
	context = context + "class " + classname + "Test : public INDEXLIB_TESTBASE" + "\n"
	context = context + "{" + "\n"
	context = context + "public:" + "\n"
	context = context + "    " + classname + "Test();" + "\n"
	context = context + "    ~" + classname + "Test();" + "\n"
	context = context + "" + "\n"
	context = context + "    DECLARE_CLASS_NAME(" + classname + "Test);" + "\n"
	context = context + "public:" + "\n"
	context = context + "    void CaseSetUp() override;" + "\n"
	context = context + "    void CaseTearDown() override;" + "\n"
	context = context + "    void TestSimpleProcess" + "();" + "\n"
	context = context + "private:" + "\n"
	context = context + "    IE_LOG_DECLARE();" + "\n"
	context = context + "};" + "\n"
	context = context + "" + "\n"
	context = context + "INDEXLIB_UNIT_TEST_CASE(" + classname + "Test, TestSimpleProcess);\n"
	context = context + "" + "\n"
	context = context + "IE_NAMESPACE_END(" + ns + ");" + "\n"
	context = context + "" + "\n"
	context = context + "#endif //__INDEXLIB_" + classname.upper() + "TEST_H" + "\n"

	f = file(path + "/" + classFileName + classSuffix + ".h", "w")
	f.write(context)
	f.close()
	return True

def createTestClassCPP(path, classFileName, classname, classSuffix):
	ns, p = getNamespace(path)
	if not ns:
		return False
	context = ""
	context = context + "#include \"indexlib/" + p + "/test/" + classFileName + classSuffix + ".h\"" + "\n"
	context = context + "" + "\n"
	context = context + "using namespace std;" + "\n"
	context = context + "IE_NAMESPACE_USE(test);" + "\n"
	context = context + "" + "\n"
	context = context + "IE_NAMESPACE_BEGIN(" + ns + ");" + "\n"
	context = context + "IE_LOG_SETUP(" + ns + ", " + classname + "Test);" + "\n"
	context = context + "" + "\n"
	context = context + classname + "Test::" + classname + "Test()" + "\n"
	context = context + "{" + "\n"
	context = context + "}" + "\n"
	context = context + "" + "\n"
	context = context + classname + "Test::~" + classname + "Test()" + "\n"
	context = context + "{" + "\n"
	context = context + "}" + "\n"
	context = context + "" + "\n"
	context = context + "void " + classname + "Test::CaseSetUp()" + "\n"
	context = context + "{" + "\n"
	context = context + "}" + "\n"
	context = context + "" + "\n"
	context = context + "void " + classname + "Test::CaseTearDown()" + "\n"
	context = context + "{" + "\n"
	context = context + "}" + "\n"
	context = context + "" + "\n"
	context = context + "void " + classname + "Test::TestSimpleProcess" + "()" + "\n"
	context = context + "{" + "\n"
	context = context + "    SCOPED_TRACE(\"Failed\");" + "\n"
	context = context + "    INDEXLIB_TEST_TRUE(false);" + "\n"
	context = context + "}" + "\n"
	context = context + "" + "\n"
	context = context + "IE_NAMESPACE_END(" + ns + ");" + "\n"
	context = context + "" + "\n"
	f = file(path + "/" + classFileName + classSuffix + ".cpp", "w")
	f.write(context)
	f.close()
	return True

def createTestClass(path, classFileName, classSuffix):
	ret = 0
	classSuffixH = classSuffix + '.h'
	classSuffixCpp = classSuffix + '.cpp'
	classname = getClassName(classFileName)
	
	if os.path.exists(path + "/" + classFileName + classSuffixH):
		print "header file " + path + "/" + classFileName + classSuffixH + " exists!" 
	else:
		if createTestClassH(path, classFileName, classname, classSuffix):
			print "create header file " + path + "/" + classFileName + classSuffixH + " succeed."
			ret = ret + 1
		else:
			print "create header file " + path + "/" + classFileName + classSuffixH + " failed."

	if os.path.exists(path + "/" + classFileName + classSuffixCpp):
		print "source file " + path + "/" + classFileName + classSuffixCpp + " exists!" 
	else:
		if createTestClassCPP(path, classFileName, classname, classSuffix):
			print "create source file " + path + "/" + classFileName + classSuffixCpp + " succeed."
			ret = ret + 1
		else:
			print "create source file " + path + "/" + classFileName + classSuffixCpp + " failed."
	return ret

def insertSConscript(scon_name, name, classSuffix):
	hasFindSource = False
	os.system("mv " + scon_name + " " + scon_name + ".bak")
	TIP = "_test_sources\s*=\s*\["
	TIP2 = "_test_sources_for_gtest\s*=\s*\["
	fin = file(scon_name + ".bak", "r");
	fout = file(scon_name, "w");
	while True:
		l = fin.readline()
		if not l:
			break;
		if not hasFindSource:
			if re.search(TIP, l):
				hasFindSource = True
			if re.search(TIP2, l):
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
        classSuffix = '_unittest'
	if len(sys.argv) == 3:
		classSuffix = sys.argv[2]

	if createTestClass(path, sys.argv[1], classSuffix) == 2:
		insertSConscript(path + "/SConscript", sys.argv[1], classSuffix)

