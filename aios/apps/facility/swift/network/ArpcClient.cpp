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
#include "swift/network/ArpcClient.h"

#include <cstddef>
#include <memory>

#include "arpc/ANetRPCChannel.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace arpc;
using namespace autil;

namespace swift {
namespace network {
AUTIL_LOG_SETUP(swift, ArpcClient);

ArpcClient::ArpcClient(const string &address, const SwiftRpcChannelManagerPtr &channelManager, const string &idStr) {
    _channelManager = channelManager;
    setAddress(address);
    _idStr = idStr;
}

ArpcClient::~ArpcClient() {}

bool ArpcClient::createNewRpcChannel() {
    _rpcChannel.reset();
    if (_address.empty()) {
        return false;
    }
    bool block = false;
    bool autoReconnect = false;
    _rpcChannel = _channelManager->openChannel(_address.c_str(), block, autoReconnect);
    if (_rpcChannel == NULL) {
        AUTIL_LOG(ERROR, "[%s] open channel on [%s] failed", _idStr.c_str(), _address.c_str());
        return false;
    } else {
        AUTIL_LOG(INFO, "[%s] open channel on [%s] success", _idStr.c_str(), _address.c_str());
        return true;
    }
}

bool ArpcClient::isChannelWorks() const {
    if (_rpcChannel != NULL && false == _rpcChannel->ChannelBroken()) {
        return true;
    }
    return false;
}

void ArpcClient::setAddress(const string &address) {
    if (StringUtil::startsWith(address, "tcp:") || StringUtil::startsWith(address, "udp:") ||
        StringUtil::startsWith(address, "http:")) {
        _address = address;
    } else {
        _address = "tcp:" + address;
    }
    _rpcChannel.reset();
}

} // namespace network
} // namespace swift
