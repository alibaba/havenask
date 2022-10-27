#!/bin/env python

import sys
import os
import re
from datetime import datetime

tmp_filepath = ('/tmp/check_before_commit.tmp.' + str(datetime.now())).replace(' ', '_')

COLOR_RED = "\033[0;31m"
COLOR_GREEN = "\033[0;32m"
COLOR_YELLOW = "\033[0;33m"
COLOR_BLUE = "\033[0;34m"
COLOR_PURPLE = "\033[0;35m"
COLOR_CYAN = "\033[0;36m"
COLOR_RESTORE = "\033[0m"

def color_line(line):
    line = line.strip()
    if not ':' in line: return line
    offset = line.find(':')
    return COLOR_CYAN + line[:offset] + COLOR_RESTORE + line[offset:]

if __name__ == "__main__":
    # get source_code_path
    if len(sys.argv) < 2:
        exe = sys.argv[0]
        print "Usage: %s code_path_root" %(exe)
        sys.exit(-1)
    
    source_code_path = os.path.abspath(sys.argv[1])

    # check unittest
    warn_count = 0
    print COLOR_PURPLE, 'checking inactive unittest in code', COLOR_RESTORE
    cmd = "cd %s; find ./ -name '*test.*' | grep -v debug64 | grep -v release64 | xargs grep -E 'INDEXLIB_UNIT_TEST_CASE|CPPUNIT_TEST' > %s" % (source_code_path, tmp_filepath)
    # print cmd
    ret = os.system(cmd)
    if ret == 0:
        for line in open(tmp_filepath):
            if '//' in line:
                print COLOR_YELLOW, 'WARN: ', COLOR_RESTORE, color_line(line)
                warn_count += 1
    if warn_count == 0:
        print COLOR_GREEN, 'SUCC (no warning found)', COLOR_RESTORE
    
    # check SConscript
    warn_count = 0
    print COLOR_PURPLE, 'checking UT Sconscript', COLOR_RESTORE
    cmd = "cd %s; find ./ -name 'SConscript' | grep -v debug64 | grep -v release64 |grep test |xargs grep 'Glob(' > %s"  % (source_code_path, tmp_filepath)
    # print cmd
    ret = os.system(cmd)
    if ret == 0:
        for line in open(tmp_filepath):
            if line.startswith('#'): continue
            for s in re.findall(r"Glob\('[^\)]*'\)", line):
                s = s.replace("Glob('", '').replace("')", '')
                LL = s.split('*')
                if len(LL) != 2 or LL[0] != '':
                    print COLOR_YELLOW, 'WARN: ', COLOR_RESTORE, color_line(line)
                    warn_count += 1
    if warn_count == 0:
        print COLOR_GREEN, 'SUCC (no warning found)', COLOR_RESTORE

    # check svn st
    warn_count = 0
    print COLOR_PURPLE, 'checking svn status', COLOR_RESTORE
    cmd = "cd %s; svn st | grep '?' > %s"  % (source_code_path, tmp_filepath)
    # print cmd
    ret = os.system(cmd)
    if ret == 0:
        for line in open(tmp_filepath):
            LL = line.split()
            if len(LL) == 2:
                if LL[1].endswith('.h') or LL[1].endswith('.cpp') or LL[1].endswith('SConscript') or LL[1].endswith('.json') or LL[1].endswith('.py'):
                    print COLOR_YELLOW, 'WARN: ', COLOR_RESTORE, color_line(line)
                    warn_count += 1
    if warn_count == 0:
        print COLOR_GREEN, 'SUCC (no warning found)', COLOR_RESTORE

    # check cout, cerr
    warn_count = 0
    print COLOR_PURPLE, 'checking debug info, such as cout|cerr', COLOR_RESTORE
    cmd = "cd %s; svn st |grep M |grep -E '\.h|\.cpp' |tr -s ' ' |cut -d ' ' -f2 |xargs grep -E 'cout|cerr' > %s" %(source_code_path, tmp_filepath)
    # print cmd
    ret = os.system(cmd)
    if ret == 0:
        for line in open(tmp_filepath):
            tmpLine = line.strip()
            if not tmpLine.startswith("//"):
                warn_count += 1
                print COLOR_YELLOW, 'WARN: ', COLOR_RESTORE, color_line(line)
    if warn_count == 0:
        print COLOR_GREEN, 'SUCC (no warning found)', COLOR_RESTORE
    
os.remove(tmp_filepath)

print 'done'
