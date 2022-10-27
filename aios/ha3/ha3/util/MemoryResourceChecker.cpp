#include <ha3/util/MemoryResourceChecker.h>
#include <autil/StringTokenizer.h>
#include <sys/sysinfo.h>
#include <fstream>

using namespace std;
using namespace autil;

#define PROC_MEMINFO_FILE  "/proc/meminfo"
#define PROC_STATM_PATH_PREFIX "/proc/"
#define PROC_STATM_PATH_SUFFIX "/statm"
#define MEM_FREE_STRING "MemFree:"
#define BUFFER_STRING "Buffers:"
#define CACHE_STRING "Cached:"

BEGIN_HA3_NAMESPACE(util);
HA3_LOG_SETUP(util, MemoryResourceChecker);

MemoryResourceChecker::MemoryResourceChecker() { 
    _memPageSize = getpagesize();
    stringstream ss;
    ss << PROC_STATM_PATH_PREFIX << getpid() << PROC_STATM_PATH_SUFFIX;
    _procStatmFile = ss.str();
}

MemoryResourceChecker::~MemoryResourceChecker() { 
}

bool MemoryResourceChecker::getFreeMemSize(int64_t memThreshold, 
        int64_t memGuardSize, int64_t& maxAvailaleMemSize) {
    int64_t resMemSize = getProcResMemSize();
    if (resMemSize == 0) {
        HA3_LOG(WARN, "get system memory info failed");
        return false;
    }
    maxAvailaleMemSize = memThreshold - resMemSize;
    return true;
}

int64_t MemoryResourceChecker::getProcResMemSize() {
    ifstream ifs(_procStatmFile.c_str());
    if (!ifs.is_open()) {
        HA3_LOG(WARN, "failed to open %s", _procStatmFile.c_str());
        return 0;
    }

    string line;
    getline(ifs, line);
    ifs.close();

    StringTokenizer st;
    st.tokenize(line, " ", StringTokenizer::TOKEN_IGNORE_EMPTY |
                StringTokenizer::TOKEN_TRIM);
    assert(st.getNumTokens() == 7);

    int64_t resPageSize = (int64_t)strtoull(st[1].c_str(), NULL, 10);
    return (int64_t)(resPageSize * _memPageSize);
}

int64_t MemoryResourceChecker::getSysFreeMemSize() {
    ifstream ifs(PROC_MEMINFO_FILE);
    if (!ifs.is_open()) {
        HA3_LOG(WARN, "failed to open %s", PROC_MEMINFO_FILE);
        return 0;
    }

    // unit in KB
    int64_t freeMem = 0;
    int64_t bufferSize = 0;
    int64_t cachedSize = 0;

    string line;
    while(!ifs.eof()) {
        getline(ifs, line);
        StringTokenizer st;
        st.tokenize(line, " ", StringTokenizer::TOKEN_IGNORE_EMPTY |
                    StringTokenizer::TOKEN_TRIM);
        assert(st.getNumTokens() == 3);
        
        if (st[0] == MEM_FREE_STRING) {
            freeMem = strtoull(st[1].c_str(), NULL, 10);
        } else if (st[0] == BUFFER_STRING) {
            bufferSize = strtoull(st[1].c_str(), NULL, 10);
        } else if (st[0] == CACHE_STRING) {
            cachedSize = strtoull(st[1].c_str(), NULL, 10);
        }

        if (freeMem != 0 && bufferSize != 0 && cachedSize != 0) {
            break;
        }
    }
    ifs.close();
    //return in Byte
    return (freeMem + bufferSize + cachedSize)*1024;
}

END_HA3_NAMESPACE(util);
