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
#include "ha3/common/WorkerAddress.h"

#include <assert.h>
#include <cstddef>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, WorkerAddress);

WorkerAddress::WorkerAddress(const string &address) {
    _address = address;
    size_t pos = _address.find(":");

    if (pos == string::npos) {
        _ip = address;
        _port = 0;
    } else {
        _ip = _address.substr(0, pos);
        _port = StringUtil::fromString<uint16_t>(_address.substr(pos + 1));
        assert(StringUtil::toString(_port) == _address.substr(pos + 1));
    }
}

WorkerAddress::WorkerAddress(const string &ip, uint16_t port) {
    _address = ip + ":" + StringUtil::toString(port);
    _ip = ip;
    _port = port;
}

WorkerAddress::~WorkerAddress() { 
}

} // namespace common
} // namespace isearch
