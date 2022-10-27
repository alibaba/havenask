#include "indexlib/util/debuging_util.h"
#include <unistd.h>
#include <ios>
#include <iostream>
#include <fstream>

using namespace std;

IE_NAMESPACE_BEGIN(util);

bool DebugingUtil::GetProcessMemUsage(double& vm_usage_in_KB, double& resident_set_in_KB)
{
    using std::ios_base;
    using std::ifstream;
    using std::string;

    vm_usage_in_KB     = 0.0;
    resident_set_in_KB = 0.0;

    // 'file' stat seems to give the most reliable results
    //
    ifstream stat_stream("/proc/self/stat", ios_base::in);
    if (stat_stream.bad())
    {
        return false;
    }

    // dummy vars for leading entries in stat that we don't care about
    //
    string pid, comm, state, ppid, pgrp, session, tty_nr;
    string tpgid, flags, minflt, cminflt, majflt, cmajflt;
    string utime, stime, cutime, cstime, priority, nice;
    string O, itrealvalue, starttime;

    // the two fields we want
    //
    unsigned long vsize;
    long rss;

    stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr
                >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
                >> utime >> stime >> cutime >> cstime >> priority >> nice
                >> O >> itrealvalue >> starttime >> vsize >> rss; // don't care about the rest

    long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
    vm_usage_in_KB     = vsize / 1024.0;
    resident_set_in_KB = rss * page_size_kb;
    return true;
}

IE_NAMESPACE_END(util);

