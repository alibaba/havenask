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

#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "swift/client/TransportClosure.h"
#include "swift/common/Common.h"
#include "swift/network/ArpcClient.h"
#include "swift/network/SwiftRpcChannelManager.h"

namespace swift {
namespace protocol {
class ConsumptionRequest;
class MessageIdRequest;
class MessageIdResponse;
class MessageResponse;
class ProductionRequest;
} // namespace protocol

namespace client {

class SwiftTransportClient : public network::ArpcClient {
public:
    SwiftTransportClient(const std::string &address,
                         const network::SwiftRpcChannelManagerPtr &channelManager,
                         const std::string &idStr = "");
    virtual ~SwiftTransportClient();

private:
    SwiftTransportClient(const SwiftTransportClient &);
    SwiftTransportClient &operator=(const SwiftTransportClient &);

public:
    virtual bool getMessage(const ::swift::protocol::ConsumptionRequest *request,
                            ::swift::protocol::MessageResponse *response,
                            int64_t timeout = 30 * 1000);
    virtual bool sendMessage(const ::swift::protocol::ProductionRequest *request,
                             ::swift::protocol::MessageResponse *response,
                             int64_t timeout = 30 * 1000);
    virtual bool getMaxMessageId(const ::swift::protocol::MessageIdRequest *request,
                                 ::swift::protocol::MessageIdResponse *response,
                                 int64_t timeout = 30 * 1000);
    virtual bool getMinMessageIdByTime(const ::swift::protocol::MessageIdRequest *request,
                                       ::swift::protocol::MessageIdResponse *response,
                                       int64_t timeout = 30 * 1000);

    template <TransportRequestType EnumType,
              typename RequestType = typename TransportRequestTraits<EnumType>::RequestType,
              typename ResponseType = typename TransportRequestTraits<EnumType>::ResponseType>
    void syncCall(const RequestType *request, ResponseType *response, int64_t timeout) {
        if constexpr (EnumType == TRT_GETMESSAGE) {
            (void)getMessage(request, response, timeout);
        } else if constexpr (EnumType == TRT_SENDMESSAGE) {
            (void)sendMessage(request, response, timeout);
        } else if constexpr (EnumType == TRT_GETMAXMESSAGEID) {
            (void)getMaxMessageId(request, response, timeout);
        } else if constexpr (EnumType == TRT_GETMINMESSAGEIDBYTIME) {
            (void)getMinMessageIdByTime(request, response, timeout);
        }
    }

    virtual void asyncCall(TransportClosureTyped<TRT_GETMESSAGE> *closure);
    virtual void asyncCall(TransportClosureTyped<TRT_SENDMESSAGE> *closure);
    virtual void asyncCall(TransportClosureTyped<TRT_GETMAXMESSAGEID> *closure);
    virtual void asyncCall(TransportClosureTyped<TRT_GETMINMESSAGEIDBYTIME> *closure);

private:
    friend class SwiftTransportClientTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftTransportClient);

} // namespace client
} // namespace swift
