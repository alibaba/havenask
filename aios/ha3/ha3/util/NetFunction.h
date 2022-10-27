#ifndef ISEARCH_NETFUNCTION_H
#define ISEARCH_NETFUNCTION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <vector>
#include <sys/ioctl.h>
#include <net/if.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

BEGIN_HA3_NAMESPACE(util);

class NetFunction
{
public:
    typedef std::vector< std::pair<std::string , in_addr> > AllIPType;
    static bool getAllIP(AllIPType & ips);
    static std::string chooseIP(const AllIPType & ips);
    static bool getPrimaryIP(std::string &ip);
    static bool containIP(const std::string &msg, const std::string &ip);
    static uint32_t encodeIp(const std::string& ip);
};


END_HA3_NAMESPACE(util);

#endif //ISEARCH_NETFUNCTION_H
