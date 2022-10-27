#ifndef __INDEXLIB_DEBUGING_UTIL_H
#define __INDEXLIB_DEBUGING_UTIL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

IE_NAMESPACE_BEGIN(util);

class DebugingUtil
{
public:
    static bool GetProcessMemUsage(double& vm_usage_in_KB, double& resident_set_in_KB);

};


IE_NAMESPACE_END(util);

#endif //__INDEXLIB_DEBUGING_UTIL_H
