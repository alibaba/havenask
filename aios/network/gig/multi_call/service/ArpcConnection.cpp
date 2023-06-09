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
#include "aios/network/gig/multi_call/service/ArpcConnection.h"
#include "aios/network/arpc/arpc/ANetRPCChannelManager.h"
#include "aios/network/arpc/arpc/RPCChannelBase.h"
#include "aios/network/gig/multi_call/interface/ArpcRequest.h"
#include "aios/network/gig/multi_call/java/GigArpcRequest.h"
#include "aios/network/gig/multi_call/java/arpc/GigArpcCallBack.h"
#include "aios/network/gig/multi_call/java/arpc/GigRPCChannel.h"
#include "aios/network/gig/multi_call/service/ProtocolCallBack.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"
#include "aios/network/gig/multi_call/util/ProtobufUtil.h"
#include <typeinfo>

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ArpcConnection);

ArpcConnection::ArpcConnection(const ANetRPCChannelManagerPtr &channelManager,
                               const std::string &spec, size_t queueSize)
    : ArpcConnectionBase(spec, MC_PROTOCOL_ARPC, queueSize),
      _channelManager(channelManager) {}

ArpcConnection::~ArpcConnection() {}

RPCChannelPtr ArpcConnection::createArpcChannel() {
    ScopedReadWriteLock lock(_channelLock, 'w');
    if (_rpcChannel && !_rpcChannel->ChannelBroken()) {
        return _rpcChannel;
    }
    auto arpcChannel = _channelManager->OpenChannel(
        _spec.c_str(), false, _queueSize, RPC_CONNECTION_TIMEOUT, false);
    if (!arpcChannel) {
        AUTIL_LOG(ERROR, "create arpc channel to %s failed", _spec.c_str());
        return RPCChannelPtr();
    }
    auto baseChannel = dynamic_cast<arpc::RPCChannelBase *>(arpcChannel);
    if (!baseChannel) {
        AUTIL_LOG(ERROR, "cast base channel failed, spec is [%s]",
                  _spec.c_str());
        return RPCChannelPtr();
    }
    _rpcChannel.reset(baseChannel);
    return _rpcChannel;
}

RPCChannelPtr ArpcConnection::getChannel() {
    {
        ScopedReadWriteLock lock(_channelLock, 'r');
        if (_rpcChannel && !_rpcChannel->ChannelBroken()) {
            return _rpcChannel;
        }
    }
    return createArpcChannel();
}

void ArpcConnection::post(const RequestPtr &request,
                          const CallBackPtr &callBack) {
    if (!request) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_REQUEST, string(), string());
        return;
    }
    auto &request_raw = *request;
    if (typeid(request_raw) != typeid(GigArpcRequest)) {
        postByStub(request, callBack);
    } else {
        postById(request, callBack);
    }
}

} // namespace multi_call
