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
#ifndef ISEARCH_MULTI_CALL_ARPCCONNECTION_H
#define ISEARCH_MULTI_CALL_ARPCCONNECTION_H

#include "aios/network/gig/multi_call/service/ArpcConnectionBase.h"
#include "autil/Lock.h"

namespace arpc {
class RPCChannelBase;
class ANetRPCChannelManager;
} // namespace arpc

namespace multi_call {

typedef std::shared_ptr<arpc::ANetRPCChannelManager> ANetRPCChannelManagerPtr;

class ArpcConnection : public ArpcConnectionBase
{
public:
    ArpcConnection(const ANetRPCChannelManagerPtr &channelManager, const std::string &spec,
                   size_t queueSize);
    ~ArpcConnection();

private:
    ArpcConnection(const ArpcConnection &);
    ArpcConnection &operator=(const ArpcConnection &);

public:
    virtual void post(const RequestPtr &request, const CallBackPtr &callBack) override;
    virtual RPCChannelPtr getChannel() override;

private:
    RPCChannelPtr createArpcChannel();

private:
    ANetRPCChannelManagerPtr _channelManager;
    autil::ReadWriteLock _channelLock;
    RPCChannelPtr _rpcChannel;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ArpcConnection);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_ARPCCONNECTION_H
