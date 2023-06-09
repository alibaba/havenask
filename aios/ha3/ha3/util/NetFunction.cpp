/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ha3/util/NetFunction.h"

#include <assert.h>
#include <ctype.h>
#include <stddef.h>
#include <sys/socket.h>
//#include "iostream"
#include <unistd.h>
#include <type_traits>

#include "arpa/inet.h"
#include "net/if.h"
#include "sys/ioctl.h"
#include "sys/socket.h"


namespace isearch {
namespace util {

bool NetFunction::getAllIP(AllIPType & ips)
{
    int fd = socket (AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }

    /* A first estimate.  */
    int rq_len = 3 * sizeof (struct ifreq);

    struct ifconf ifc;
    ifc.ifc_buf = NULL;
    ifc.ifc_len = 0;

    /* Read all the interfaces out of the kernel.  */
    std::vector<char> buffer;
    do {
        buffer.clear();
        buffer.resize(ifc.ifc_len = rq_len);
        ifc.ifc_buf = &*buffer.begin();
        if (ifc.ifc_buf == NULL || ioctl (fd, SIOCGIFCONF, &ifc) < 0) {
            close (fd);
            return false;
        }
        rq_len *= 2;
    } while (ifc.ifc_len == (int)buffer.size());

    for(struct ifreq * cur = (struct ifreq *) ifc.ifc_req;
        (char *)cur < (char *)ifc.ifc_req + ifc.ifc_len;
        ++cur)
    {
        if (cur->ifr_addr.sa_family == AF_INET) {
            struct sockaddr_in * sa = (struct sockaddr_in *)&cur->ifr_addr;
            ips.push_back(std::make_pair(cur->ifr_name, sa->sin_addr));
//          std::cout << "inet " << cur->ifr_name << " " << inet_ntoa(sa->sin_addr) << std::endl;
        }
    }

    close (fd);
    return true;
}

struct InnerIpListType
{
    const char * ip;
    const char * mask;
};

static InnerIpListType innerIPList[] = {
    {"255.255.255.255","255.255.255.255"},
    {"240.0.0.0","240.0.0.0"},
    {"224.0.0.0","240.0.0.0"},
    {"192.88.99.0","255.255.255.255"},
    {"127.0.0.0","255.0.0.0"},
    {"169.254.0.0","255.255.0.0"},
    {"192.168.0.0","255.255.0.0"},
    {"172.16.0.0","255.240.0.0"},
    {"10.0.0.0","255.0.0.0"}
};

class InnerIpContainer
{
private:
    typedef std::vector< std::pair<in_addr, in_addr> > IpVectorType;
    InnerIpContainer() {
        for(unsigned int j = 0; j < sizeof(innerIPList)/sizeof(innerIPList[0]); ++j) {
            in_addr ip,mask;
            if(!inet_aton(innerIPList[j].ip, &ip)) {
                assert(0);
                continue;
            }
            if(!inet_aton(innerIPList[j].mask, &mask)) {
                assert(0);
                continue;
            }
            _ipVector.push_back(std::make_pair(ip, mask));
        }
    }
public:
    static InnerIpContainer * getContainer() {
        static InnerIpContainer me;
        return &me;
    }
    unsigned int getWeight(in_addr ip) const {
        unsigned int weight = 0;
        for(IpVectorType::const_iterator i = _ipVector.begin(); i != _ipVector.end(); ++i, ++weight) {
//            std::cout << std::hex << i->second.s_addr << " " << ip.s_addr << " " << i->first.s_addr << std::endl;
            if((i->second.s_addr & ip.s_addr) == i->first.s_addr) {
                // match
                return weight;
            }
        }
        return weight;
    }
private:
    IpVectorType _ipVector;
};

std::string NetFunction::chooseIP(const AllIPType & ips)
{
    in_addr ip;
    if(!inet_aton("127.0.0.1", &ip)) {
        assert(0);
        return "127.0.0.1";
    }
    unsigned int weight = InnerIpContainer::getContainer()->getWeight(ip);
    
    for(AllIPType::const_iterator i = ips.begin(); i != ips.end(); ++i) {
        unsigned int newWeight = InnerIpContainer::getContainer()->getWeight(i->second);
        if(newWeight > weight) {
            ip = i->second;
            weight = newWeight;
        }
    }
    return inet_ntoa(ip);
}

bool NetFunction::getPrimaryIP(std::string &ip)
{
    AllIPType ips;
    if(getAllIP(ips)) {
        ip = chooseIP(ips);
        return true;
    }
    return false;
}

bool NetFunction::containIP(const std::string &msg, 
                            const std::string &ip) 
{
    if (ip.empty()) {
        return true;
    }

    size_t pos = 0;
    size_t ipSz = ip.size();
    while ((pos = msg.find(ip, pos)) 
           != std::string::npos) 
    {
        if ((pos == 0 || !isdigit(msg[pos - 1]))
            && (pos + ipSz >= msg.size() || !isdigit(msg[pos + ipSz])))
        {
            return true;
        } 

        pos += ipSz;
    }

    return false;
}

uint32_t NetFunction::encodeIp(const std::string& ip) {
    return ntohl(inet_addr(ip.c_str()));
}

} // namespace util
} // namespace isearch

