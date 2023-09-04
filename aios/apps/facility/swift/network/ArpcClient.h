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
#pragma once

#include <string>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftRpcChannelManager.h"

namespace swift {
namespace network {

class ArpcClient {
public:
    ArpcClient(const std::string &address,
               const SwiftRpcChannelManagerPtr &channelManager,
               const std::string &idStr = "");
    virtual ~ArpcClient();

private:
    ArpcClient(const ArpcClient &);
    ArpcClient &operator=(const ArpcClient &);

public:
    void setAddress(const std::string &address);
    const std::string &getAddress() const { return _address; }

protected:
    bool checkRpcChannel() {
        if (isChannelWorks()) {
            return true;
        } else {
            return createNewRpcChannel();
        }
    }

    bool createNewRpcChannel();
    bool isChannelWorks() const;

protected:
    std::string _address;
    ANetRpcChannelPtr _rpcChannel;
    SwiftRpcChannelManagerPtr _channelManager;
    std::string _idStr;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ArpcClient);

} // namespace network
} // namespace swift
