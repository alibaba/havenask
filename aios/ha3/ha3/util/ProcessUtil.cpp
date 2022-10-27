#include <ha3/util/ProcessUtil.h>
#include <autil/StringTokenizer.h>
#include <fstream>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(util);
HA3_LOG_SETUP(util, ProcessUtil);

ProcessUtil::ProcessUtil() { 
}

ProcessUtil::~ProcessUtil() { 
}

string ProcessUtil::getProcessName(pid_t pid) {
    stringstream ss;
    ss << "/proc/" << pid << "/status";

    string filePath = ss.str();

    ifstream in(filePath.c_str());

    string line;
    if (!in) {
        HA3_LOG(WARN, "Wrong FilePath:[%s]", filePath.c_str());
        return string("");
    }
    
    if (!getline(in, line)) {
        HA3_LOG(WARN, "Fail to get the line from %s", filePath.c_str());
        return string("");
    }

    StringTokenizer tokens(line, ":", StringTokenizer::TOKEN_TRIM);
    if (tokens.getNumTokens() < 2) {
        HA3_LOG(WARN, "Fail to get the process name");
        return string("");
    }

    string processName = tokens[1];

    in.close();

    return processName;
}

string ProcessUtil::getCurProcessName() {
    pid_t curPid = getpid();
    return getProcessName(curPid);
}

END_HA3_NAMESPACE(util);

