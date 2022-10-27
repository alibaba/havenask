#!/bin/env python

import sys
import os
from newclass import *
from modify_class import *

usage = '''Usage:
    class_util [operate] <class_name> [method_name]
operate := [add_class|add_method]
'''

def PrintUsage():
	print usage
	sys.exit(1)

if __name__ == "__main__":
	if len(sys.argv) <= 2:
		PrintUsage()

	path = os.getcwd()
	operate = sys.argv[1]
	className = sys.argv[2]
	if operate == 'add_class':
		createClass(path, className)
	elif operate == 'add_method':
		if len(sys.argv) != 4:
			PrintUsage()
		memberName = sys.argv[3]
		indexlibClass = IndexlibClass(path, className)
		indexlibClass.addMethod(memberName)
	else:
		assert(False)
