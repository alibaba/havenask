#!/bin/env python

import sys
import os

def getNamespace(path):
	'''
	ls = path.split("/")
	n = len(ls)
	while n >= 1 and len(ls[n-1]) == 0:
		n = n - 1
	if n < 1:
		return None
	ans = ls[n-1]
	'''
	if 'indexlib/' not in path:
		return "NONE"
	LLL = path[path.rfind('indexlib/') + len('indexlib/') : ].split("/")
	if not LLL or not LLL[0]: 
		return "NONE"
	return LLL[0]
		

def getClassName(path):
	LLL = path.split('_')
	for i in range(len(LLL)): LLL[i] = LLL[i][0].upper() + LLL[i][1:]
	return ''.join(LLL)

def createClassH(path, classname):
	ns = getNamespace(path)
	if not ns:
		return False
	context = ""
	context = context + "#ifndef __INDEXLIB_" + classname.upper() + "_H" + "\n"
	context = context + "#define __INDEXLIB_" + classname.upper() + "_H" + "\n"
	context = context + "" + "\n"
	context = context + "#include <tr1/memory>" + "\n"
	context = context + "#include \"indexlib/indexlib.h\"" + "\n"
	context = context + "#include \"indexlib/common_define.h\"" + "\n"
	context = context + "" + "\n"
	context = context + "IE_NAMESPACE_BEGIN(" + ns + ");" + "\n"
	context = context + "" + "\n"
	context = context + "class " + getClassName(classname) + "\n"
	context = context + "{" + "\n"
	context = context + "public:" + "\n"
	context = context + "    " + getClassName(classname) + "();" + "\n"
	context = context + "    ~" + getClassName(classname) + "();" + "\n"
	context = context + "public:" + "\n"
	context = context + "" + "\n"
	context = context + "private:" + "\n"
	context = context + "    IE_LOG_DECLARE();" + "\n"
	context = context + "};" + "\n"
	context = context + "" + "\n"
	context = context + "DEFINE_SHARED_PTR(" + getClassName(classname) + ");\n\n"
	context = context + "IE_NAMESPACE_END(" + ns + ");" + "\n"
	context = context + "" + "\n"
	context = context + "#endif //__INDEXLIB_" + classname.upper() + "_H" + "\n"
	f = file(path + "/" + classname + ".h", "w")
	f.write(context)
	f.close()
	return True

def createClassCPP(path, classname):
	ns = getNamespace(path)
	if not ns:
		return False
	context = ""
	context = context + "#include \"indexlib/" + ns + "/" + classname + ".h\"" + "\n"
	context = context + "" + "\n"
	context = context + "using namespace std;" + "\n\n"
	context = context + "IE_NAMESPACE_BEGIN(" + ns + ");" + "\n"
	context = context + "IE_LOG_SETUP(" + ns + ", " + getClassName(classname) + ");" + "\n"
	context = context + "" + "\n"
	context = context + getClassName(classname) + "::" + getClassName(classname) + "() " + "\n"
	context = context + "{" + "\n"
	context = context + "}" + "\n"
	context = context + "" + "\n"
	context = context + getClassName(classname) + "::~" + getClassName(classname) + "() " + "\n"
	context = context + "{" + "\n"
	context = context + "}" + "\n"
	context = context + "" + "\n"
	context = context + "IE_NAMESPACE_END(" + ns + ");" + "\n"
	context = context + "" + "\n"
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
		print "header file " + path + "/" + classname + ".cpp exists!" 
	else:
		if createClassCPP(path, classname):
			print "create header file " + path + "/" + classname + ".cpp succeed."
			ret = ret + 1
		else:
			print "create header file " + path + "/" + classname + ".cpp failed."
	return ret

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print >> sys.stderr, "Usage:", sys.argv[0], "<classname>"
		sys.exit(1)
	path = os.getcwd()
	if createClass(path, sys.argv[1]) == 2:
		pass
