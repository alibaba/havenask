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
#include "suez/sdk/IpUtil.h"

#include <arpa/inet.h>
#include <cstddef>
#include <netdb.h>
#include <stdint.h>
#include <sys/socket.h>
#include <unistd.h>

#include "autil/Lock.h"

using namespace std;

namespace suez {

string IpUtil::_hostAddress;
autil::ThreadMutex IpUtil::_lock;

IpUtil::IpUtil() {}

IpUtil::~IpUtil() {}

string IpUtil::doGetHostAddress() {
    char hname[128];
    struct hostent hentBuffer;
    struct hostent *hent = NULL;
    char buf[1024];
    int h_err;
    gethostname(hname, sizeof(hname));
    gethostbyname_r(hname, &hentBuffer, buf, sizeof(buf), &hent, &h_err);
    if (!hent) {
        return string();
    }
    for (uint32_t i = 0; hent->h_addr_list[i]; i++) {
        char IpChars[128];
        const char *ret = inet_ntop(AF_INET, hent->h_addr_list[i], IpChars, 128);
        if (ret) {
            return string(ret);
        }
    }
    return string();
}

string IpUtil::getHostAddress() {
    autil::ScopedLock lock(_lock);
    if (_hostAddress.empty()) {
        _hostAddress = doGetHostAddress();
    }
    return _hostAddress;
}

} // namespace suez
