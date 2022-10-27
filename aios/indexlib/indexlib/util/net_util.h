#ifndef __INDEXLIB_NET_UTIL_H
#define __INDEXLIB_NET_UTIL_H

#include <string>
#include <tr1/memory>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

IE_NAMESPACE_BEGIN(util);

class NetUtil
{
public:
    static bool GetIp(std::vector<std::string>& ips);
    static bool GetDefaultIp(std::string& ip);
    static bool GetHostName(std::string& hostname);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NetUtil);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_NET_UTIL_H
