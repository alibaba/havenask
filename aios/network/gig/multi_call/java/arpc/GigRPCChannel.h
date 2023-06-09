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
#ifndef ISEARCH_MULTI_CALL_GIGRPCCHANNEL_H
#define ISEARCH_MULTI_CALL_GIGRPCCHANNEL_H

#include "aios/network/arpc/arpc/ANetRPCChannel.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/java/arpc/GigClientPacketHandler.h"
#include "aios/network/gig/multi_call/java/arpc/GigRPCArg.h"
#include "aios/network/gig/multi_call/java/arpc/GigRPCMessageCodec.h"
#include <string>

namespace multi_call {

class GigRPCChannel : public arpc::ANetRPCChannel {
public:
    GigRPCChannel(anet::Connection *pConnection,
                  GigRPCMessageCodec *gigMessageCodec, bool block = false);
    ~GigRPCChannel();

private:
    GigRPCChannel(const GigRPCChannel &);
    GigRPCChannel &operator=(const GigRPCChannel &);

public:
    using arpc::ANetRPCChannel::CallMethod;
    void CallMethod(uint32_t serviceId, uint32_t methodId,
                    RPCController *controller, const std::string *request,
                    std::string *response, RPCClosure *done);
    bool CheckResponsePacket(anet::Packet *packet, GigRpcReqArg *pArgs);

private:
    void SyncCall(anet::Packet *pPack, GigRpcReqArg *pArg);
    bool AsyncCall(anet::Packet *pPack, GigRpcReqArg *pArg);
    bool needRepostPacket(arpc::ErrorCode errorCode, version_t remoteVersion,
                          version_t postedPacketVersion) const;
    void SetError(arpc::ANetRPCController *pController,
                  arpc::ErrorCode errorCode);
    void RunClosure(RPCClosure *pClosure);

private:
    GigRPCMessageCodec *_gigMessageCodec;
    GigClientPacketHandler *_gigClientPacketHandler;

private:
    version_t _version;
    bool _enableTrace;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigRPCChannel);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGRPCCHANNEL_H
