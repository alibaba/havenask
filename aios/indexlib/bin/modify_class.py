#!/bin/env python

import sys
import os

METHOD_TEMPLATE = '''
    void Set%(MethodName)s(const %(MemberType)s& %(ParamName)s)
    { %(MemberName)s = %(ParamName)s; }
    const %(MemberType)s& Get%(MethodName)s() const;
    { return %(MemberName)s; }

'''

class IndexlibClass(object):
	def __init__(self, path, className):
		self._outputFilePath = os.path.join(path, className)
		self._initClassFile(path, className)
		pass

	def addMethod(self, memberName):
		for line in self.headerFileLines:
			pos = line.find(memberName)
			if pos != -1:
				self._doAddMethod(line)
				break
		self._outputFile(self._outputFilePath)

	def _doAddMethod(self, line):
		MemberType, MemberName, MethodName, ParamName = self._parseLine(line)
		content = METHOD_TEMPLATE % {'MemberName' : MemberName,
					     'MemberType' : MemberType,
					     'MethodName' : MethodName,
					     'ParamName' : ParamName}
		pos = 0
		for line in self.headerFileLines:
			if line.strip() == 'private:':
				self.headerFileLines.insert(pos, content)
				break
			pos = pos + 1
	
	def _initClassFile(self, path, className):
		headerFile = file(os.path.join(path, className + ".h"), "r")
		self.headerFileLines = headerFile.readlines()
		headerFile.close()

		cppFile = file(os.path.join(path, className + ".cpp"), "r")
		self.cppFileLines = cppFile.readlines()
		cppFile.close()

	def _outputFile(self, path):
		print ''.join(self.headerFileLines)
		headerFile = file(self._outputFilePath + ".h", "w")
		headerFile.writelines(self.headerFileLines)


	def _parseLine(self, line):
		tokenList = line.rstrip(';\n').split()
		assert(len(tokenList) == 2)
		memberName = tokenList[1]
		methodName = memberName.lstrip('m')
		firstChar = methodName[0].lower()
		paramName = firstChar + methodName[1:]

		return tokenList[0], memberName, methodName, paramName
	
