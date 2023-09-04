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
#include "aios/network/anet/appadapter.h"

#include <iosfwd>

#include "aios/network/anet/connection.h"
#include "aios/network/anet/transport.h"

namespace anet {
class IConnection;
class IPacketStreamer;
} // namespace anet

using namespace std;

namespace anet {

AnetAcceptor::AnetAcceptor(
    const string &address, Transport *transport, IPacketStreamer *streamer, int postPacketTimeout, int maxIdleTime)
    : mAddress(address)
    , mTransport(transport)
    , mStreamer(streamer)
    , mPostPacketTimeout(postPacketTimeout)
    , mMaxIdleTime(maxIdleTime) {}

void AnetAcceptor::HandleAccept() {
    mTransport->listen(mAddress.c_str(), mStreamer, this, mPostPacketTimeout, mMaxIdleTime);
}

AnetConnector::AnetConnector(const string &address, Transport *transport, IPacketStreamer *streamer, bool autoReconn)
    : mAddress(address), mTransport(transport), mStreamer(streamer), mAutoReconn(autoReconn) {}

IConnection *AnetConnector::Connect() { return mTransport->connect(mAddress.c_str(), mStreamer, mAutoReconn); }

} // namespace anet
