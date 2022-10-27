#include "indexlib/util/net_util.h"

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <string>
#include <vector>

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, NetUtil);

bool NetUtil::GetIp(std::vector<std::string>& ips)
{
    std::string hostName;
    if (!GetHostName(hostName))
    {
        return false;
    }
    struct hostent* hent;
    hent = gethostbyname(hostName.c_str());
    for (uint32_t i = 0; hent->h_addr_list[i]; i++)
    {
        std::string ip = inet_ntoa(*(struct in_addr*)(hent->h_addr_list[i]));
        ips.push_back(ip);
    }
    return true;
}

bool NetUtil::GetDefaultIp(std::string& ip)
{
    std::vector<std::string> ips;
    if (!GetIp(ips))
    {
        return false;
    }
    if (ips.empty())
    {
        return false;
    }
    ip.assign(ips[0]);
    return true;
}

bool NetUtil::GetHostName(std::string& hostname)
{
    char buf[128];
    if (0 == gethostname(buf, sizeof(buf)))
    {
        hostname.assign(buf);
        return true;
    }
    return false;
}

IE_NAMESPACE_END(util);
