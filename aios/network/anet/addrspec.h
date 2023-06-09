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
#ifndef ADDR_SPEC_H_
#define ADDR_SPEC_H_

#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/un.h>
#include <stdint.h>

#include "aios/network/anet/debug.h"

namespace anet {

/* macro for the domain socket file path prefix. */
#define DOMAIN_SOCK_PATH "/tmp/"

typedef union
{
    struct sockaddr_un un;
    struct sockaddr_in in;    
} UnitedAddr;

/* This class encapsulates the 'spec' parsing work in ANET,
 * and seperating the spec string into real inet/unix _address.
 * For spec string, the following format is recognized:
 * 1) tcp:<ip>:port
 * 2) udp:<ip>:port
 * 3) unixstream:<domain socket path>
 * 4) unixdgram:<domain socket path>
 */
class AddrSpec
{
public:
    AddrSpec(){
        _isValid = false;
        memset(&_addr, 0, sizeof(UnitedAddr));
    }
    AddrSpec(const char *spec){ (void)setAddrSpec(spec); }
    ~AddrSpec(){}
    
    /* tool function to check if the str is num dot format ip address. */
    bool isIpAddrStr(const char * str);
    /* tool function to get network ip address from a string .*/
    int getIpByHost(const char *address, struct sockaddr_in *in);

    /* Initiate from inet address, with hostname as the input. */
    int setInetAddr(int protocolType, const char * addrStr, int port);
    /* Initiate from inet address, with host address as input. */
    int setInetAddr(int protocolType, struct sockaddr_in *newAddr) { 
        _type = protocolType;
        memcpy(&(_addr.in), newAddr, sizeof (*newAddr));
        _isValid = true;
        return _isValid;
    }
    /* Initiate from unix domain socket path. */
    int setUnixAddr(int protocolType, const char *unixPath); 
    void setAddr(int family, int protocolType, struct sockaddr *address) { 
        setProtocolFamily(family);
        setProtocolType(protocolType);
        memcpy(&_addr, address, getAddrSize());
    }

    /* Initiate from a spec string (like tcp:ip:port). */
    int setAddrSpec(const char *spec);

    void setProtocolFamily(int family) { ((struct sockaddr *)(&_addr))->sa_family = family; }
    int  getProtocolFamily() { return ((struct sockaddr *)(&_addr))->sa_family; }
    void setProtocolType(int protocolType) { _type = protocolType; }
    int  getProtocolType() { return _type; }
    
    int getAddrSize() { 
        if (_addr.in.sin_family == AF_INET) return sizeof(struct sockaddr_in);
        if (_addr.in.sin_family == AF_UNIX) return sizeof(struct sockaddr_un);
        return 0;
    }
    struct sockaddr * getAddr() { return (struct sockaddr *)&_addr; }
    UnitedAddr * getUnitedAddr() { return &_addr; }
    struct sockaddr_in * getInAddr() { return &_addr.in; }
    struct sockaddr_un * getUnAddr() { return &_addr.un; }
    uint32_t getNetworkAddr() { return _addr.in.sin_addr.s_addr; }
    uint16_t getPort() { return ntohs(_addr.in.sin_port); };
    char *   getUnixPath() { return _addr.un.sun_path; }

    bool isValidState() { return _isValid; }

protected:
    int parseSpec(char *src, char **args, int cnt);
    
private:
    /* Indicate if the spec is valid, and if it has been parsed 
     * successfully. */
    bool _isValid;

    int _type;
    UnitedAddr _addr; 
};

}
#endif
