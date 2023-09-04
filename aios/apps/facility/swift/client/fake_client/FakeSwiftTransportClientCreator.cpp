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
#include <iosfwd>
#include <map>
#include <string>
#include <vector>

#include "swift/client/SwiftTransportClientCreator.h"
#include "swift/client/fake_client/FakeSwiftTransportClient.h"
#include "swift/network/SwiftRpcChannelManager.h"

namespace swift {
namespace client {
class SwiftTransportClient;
} // namespace client
namespace protocol {
class Message;
} // namespace protocol
} // namespace swift

using namespace std;

namespace swift {
namespace client {

bool SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = false;
std::map<std::string, std::vector<swift::protocol::Message>> SwiftTransportClientCreator::_fakeDataMap;

SwiftTransportClient *SwiftTransportClientCreator::createTransportClient(
    const std::string &address, const network::SwiftRpcChannelManagerPtr &channelManager, const std::string &idStr) {
    FakeSwiftTransportClient *fakeClient = new FakeSwiftTransportClient(address, channelManager, idStr);
    fakeClient->setFakeDataMap(&_fakeDataMap);
    if (FAKE_CLIENT_AUTO_ASYNC_CALL) {
        fakeClient->setAutoAsyncCall();
    }
    return fakeClient;
}

} // namespace client
} // namespace swift
