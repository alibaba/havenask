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
#include "autil/NetUtil.h"

#include <algorithm>
#include <arpa/inet.h>
#include <cstring>
#include <fstream> // IWYU pragma: keep
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

namespace autil {
AUTIL_LOG_SETUP(autil, NetUtil);

const std::string NetUtil::HOST_INFO_PATH("/etc/hostinfo");

bool NetUtil::GetIp(std::vector<std::string> &ips) {
    std::string hostName;
    if (!GetHostName(hostName)) {
        return false;
    }
    struct hostent hentBuffer;
    struct hostent *hent = nullptr;
    ::memset(&hentBuffer, 0, sizeof(hentBuffer));

    hent = gethostbyname(hostName.c_str());
    int hError = 0;
    char buf[1024];
    if (gethostbyname_r(hostName.c_str(), &hentBuffer, buf, sizeof(buf), &hent, &hError) != 0) {
        AUTIL_LOG(WARN, "gethostbyname failed, hostname [%s], h_errno [%d]", hostName.c_str(), hError);
        return false;
    }
    if (!hent) {
        AUTIL_LOG(WARN, "gethostbyname failed, hostname [%s], null hostent", hostName.c_str());
        return false;
    }

    for (uint32_t i = 0; hent->h_addr_list[i]; i++) {
        char ipChars[128];
        const char *ret = inet_ntop(AF_INET, hent->h_addr_list[i], ipChars, 128);
        if (ret) {
            ips.push_back(std::string(ret));
        }
    }
    return true;
}

bool NetUtil::GetDefaultIp(std::string &ip) {
    std::vector<std::string> ips;
    if (!GetIp(ips)) {
        return false;
    }
    if (ips.empty()) {
        return false;
    }
    ip.assign(ips[0]);
    return true;
}

bool NetUtil::GetHostName(std::string &hostname) {
    char buf[128];
    if (0 == gethostname(buf, sizeof(buf))) {
        hostname.assign(buf);
        return true;
    }
    return false;
}

uint16_t NetUtil::getPort(uint16_t from, uint16_t to) {
    static unsigned int serial = 0;
    srand((unsigned int)(autil::TimeUtility::currentTime()) + (unsigned int)(getpid()) + serial++);
    auto port = (rand() % (to - from)) + from;
    return (uint16_t)port;
}

uint16_t NetUtil::randomPort() {
    while (true) {
        uint16_t port = getPort();
        int testSock;
        testSock = socket(AF_INET, SOCK_STREAM, 0);
        if (testSock == -1) {
            continue;
        }

        struct sockaddr_in my_addr;
        my_addr.sin_family = AF_INET;
        my_addr.sin_port = htons(port);
        my_addr.sin_addr.s_addr = INADDR_ANY;
        bzero(&(my_addr.sin_zero), sizeof(my_addr.sin_zero));

        if (bind(testSock, (struct sockaddr *)&my_addr, sizeof(struct sockaddr)) == -1) {
            close(testSock);
            continue;
        }
        close(testSock);
        return port;
    }
}

std::string NetUtil::inetNtoa(int32_t ip) {
    char strTemp[20];
    if (ip == -1) {
        return "";
    }
    uint32_t nValue = (uint32_t)ip;
    sprintf(strTemp,
            "%d.%d.%d.%d",
            (nValue & 0x000000ff),
            (nValue & 0x0000ff00) >> 8,
            (nValue & 0x00ff0000) >> 16,
            (nValue & 0xff000000) >> 24);
    return std::string(strTemp);
}

std::string _hostAddress;
autil::ThreadMutex _lock;

// copy from build_service
std::string NetUtil::getBindIp() {
    autil::ScopedLock lock(_lock);
    if (_hostAddress.empty()) {
        std::string ip;
        if (GetDefaultIp(ip)) {
            _hostAddress = ip;
        }
    }
    return _hostAddress;
}

const std::string RES_NAME_IP_SEPARATOR = ":";
// simply judge whether hostNic is mac address
bool NetUtil::isMacAddr(const std::string &addr) {
    std::vector<std::string> items = autil::StringUtil::split(addr, RES_NAME_IP_SEPARATOR);
    if (items.size() == 6) {
        return true;
    }
    return false;
}

bool NetUtil::GetMachineName(std::string &host) {
    std::ifstream hostinfo(HOST_INFO_PATH.c_str());
    if (!hostinfo.is_open()) {
        return false;
    }
    std::string line;
    while (getline(hostinfo, line)) {
        if (line.length() > 0) {
            host = line;
            return true;
        }
    }
    return false;
}

} // namespace autil
