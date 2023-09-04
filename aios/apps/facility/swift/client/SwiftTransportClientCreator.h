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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "swift/network/SwiftRpcChannelManager.h"

namespace swift {
namespace storage {
class MessageBrain;
}
} // namespace swift

namespace swift {
namespace protocol {
class Message;
} // namespace protocol

namespace client {
class SwiftTransportClient;

class SwiftTransportClientCreator {
public:
    static SwiftTransportClient *createTransportClient(const std::string &address,
                                                       const network::SwiftRpcChannelManagerPtr &channelManager,
                                                       const std::string &idStr);

public:
    typedef std::shared_ptr<swift::storage::MessageBrain> MessageBrainPtr;

public:
    static bool FAKE_CLIENT_AUTO_ASYNC_CALL;
    static std::map<std::string, std::vector<swift::protocol::Message>> _fakeDataMap;
    static std::map<std::string, MessageBrainPtr> _msgBrainMap;
    static std::string _fakeDataPath;
};

} // namespace client
} // namespace swift
