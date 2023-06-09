import sys
import string
def doGenerateCppErrorInfo(source, target):
    oriFile = source
    cppFile = target
    hFile = cppFile.replace(".cpp", ".h");
    ERRORINFO_START_FLAG = "%{\n"
    ERRORINFO_END_FLAG = "}%\n"
    ERRORCODE_FIRST_FLAG = "%{error_code_first}%\n"
    FILE_SEPERATOR = "%file_seperator%\n"

    cppContent = ""
    hContent = ""
    isErrorCodeFirst = False
    isErrorCode = False
    isCppContent = False
    cppCode = ""
    errorCodeMap = {}
    lineNum = 0;
    try:
        f = open(oriFile, 'r');
        fh = open(hFile, 'w');
        fcpp = open(cppFile, 'w');
        for line in f:
            lineNum = lineNum + 1
            if line.strip() == '':
                continue
            if line == ERRORINFO_START_FLAG:
                isErrorCode = True
                continue
            elif line == ERRORINFO_END_FLAG:
                isErrorCode = False
                continue
            elif line == FILE_SEPERATOR:
                isCppContent = True
                continue
            elif line == ERRORCODE_FIRST_FLAG:
                isErrorCodeFirst = True
                continue
            if (isErrorCode) :
                list = string.split(s=line.strip(), maxsplit=2)
                size = len(list)
                if (size < 2) :
                    print "Generate python errorInfo failed: source [%s], Error at %s" % (oriFile, line)
                    f.close()
                    fh.close()
                    fcpp.close()
                    return False
                if isErrorCodeFirst:
                    errorDefine = list[1]
                    errorCode = list[0]
                else :
                    errorDefine = list[0]
                    errorCode = list[1]
                if (size >= 3) :
                    errorMsg = list[2]
                    if errorMsg != "" and (len(errorMsg) < 2
                                           or errorMsg[0] != '"'
                                           or errorMsg[-1] != '"'):
                        print "errorMsg should startswith \" and endswith \""
                        print "Generate python errorInfo failed: source [%s]. Error at %s" % (oriFile, line)

                    if errorMsg == "" :
                        errorMsg = '"' + errorDefine + '"'
                else:
                    errorMsg = '"' + errorDefine + '"'

                if errorCode in errorCodeMap:
                    print "%s:%d: warning: errorCode duplicated [%s]" % (oriFile, lineNum, errorCode)
                errorCodeMap[errorCode] = ""
                hCode = "constexpr ErrorCode " + errorDefine + " = " + errorCode + ";\n"
                hContent += hCode;
                cppCode += "    gCode2MsgMap[" +  errorDefine + "] = " + errorMsg + ";\n"
            elif isCppContent:
                cppContent += line
            else:
                hContent += line

        cppContent = cppContent % {'ERRORINFO': cppCode}
        fh.write(hContent)
        fcpp.write(cppContent)
        f.close()
        fh.close()
        fcpp.close()
    except Exception, e:
        print e
        print "Generate python errorInfo failed: source [%s], target [%s]." % (source, target)
        return False
    return 0

def doGeneratePythonErrorInfo(source, target):
    ERRORINFO_START_FLAG = "%{\n"
    ERRORINFO_END_FLAG = "}%\n"
    ERRORCODE_FIRST_FLAG = "%{error_code_first}%\n"
    content = "# -*- mode: python -*-\n"
    isErrorCode = False
    isErrorCodeFirst = False
    try:
        sourceFile = source
        targetFile = target.replace(".cpp", ".py")
        sf = open(sourceFile, 'r');
        tf = open(targetFile, 'w');
        for line in sf:
            if line.strip() == '':
                continue
            if line == ERRORINFO_START_FLAG:
                isErrorCode = True
                continue
            elif line == ERRORINFO_END_FLAG:
                isErrorCode = False
                continue
            elif line == ERRORCODE_FIRST_FLAG:
                isErrorCodeFirst = True
                continue
            if (isErrorCode) :
                list = string.split(s=line.strip(), maxsplit=2)
                size = len(list)
                if (size < 2) :
                    print "Generate python errorInfo failed: source [%s], target [%s]. Error at %s" % (sourceFile, targetFile, line)
                    sf.close()
                    tf.close()
                    return False
                if isErrorCodeFirst:
                    errorDefine = list[1]
                    errorCode = list[0]
                else :
                    errorDefine = list[0]
                    errorCode = list[1]
                content += errorDefine + " = " + errorCode + "\n"
        tf.write(content)
        sf.close()
        tf.close()
    except Exception, e:
        print e
        print "Generate python errorInfo failed: source [%s], target [%s]." % (sourceFile, targetFile)
        return False
    return 0

doGenerateCppErrorInfo(sys.argv[1], sys.argv[2])
doGeneratePythonErrorInfo(sys.argv[1], sys.argv[2])
