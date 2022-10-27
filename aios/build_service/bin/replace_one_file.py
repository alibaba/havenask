import sys
import re
classname = sys.argv[1]

f = open(classname + '.h', 'r')
lines = f.readlines()
f.close()

start = False
classdeclare = []
headers = []
for idx, line in enumerate(lines):
    if line.startswith('#include'):
        headers.append(line)
    if line.startswith('BEGIN_BS_NAMESPACE'):
        start = True
        continue
    if line.startswith('END_BS_NAMESPACE'):
        start = False
    if line.startswith('private:'):
        line = "protected:\n"
    if not re.match(".*void test.*\(\).*", line) and start:
        classdeclare.append(line)


f = open(classname + '.cpp', 'r')
lines = f.readlines()
f.close()

newlines = []
for idx, line in enumerate(lines):
    if line.find(classname+'.h') != -1:
        newlines += headers;
        continue
    if line.startswith('BEGIN_BS_NAMESPACE'):
        newlines.append(line)
        newlines += classdeclare
        continue
    if line.startswith('void ' + classname + '::test'):
        line = line.replace("void ", "TEST_F(")
        line = line.replace("::", ", ")
        line = line.replace("()", ")")
        newlines.append(line)
        continue
    if line.startswith('BUILD_SERVICE_UNIT_TEST_CASE'):
        continue
    newlines.append(line)

f = open(classname + '.cpp', 'w')
f.writelines(newlines)
f.close()
    

