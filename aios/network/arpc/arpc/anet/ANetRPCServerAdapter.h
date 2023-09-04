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

#include "aios/network/anet/anet.h"
#include "aios/network/arpc/arpc/RPCServerAdapter.h"
#include "aios/network/arpc/arpc/anet/ANetRPCMessageCodec.h"
#include "aios/network/arpc/arpc/metric/ServerMetricReporter.h"

ARPC_BEGIN_NAMESPACE(arpc)

class ANetRPCServerAdapter : public RPCServerAdapter, public anet::IServerAdapter {
public:
    ANetRPCServerAdapter(RPCServer *pRpcServer, ANetRPCMessageCodec *_messageCodec);
    virtual ~ANetRPCServerAdapter();

public:
    anet::IPacketHandler::HPRetCode handlePacket(anet::Connection *pConnection, anet::Packet *pPacket);
    void SetMetricReporter(const std::shared_ptr<ServerMetricReporter> &metricReporter);

private:
    void handleError(anet::Connection *pConnection,
                     anet::DelayDecodePacket *pReqPacket,
                     ErrorCode errCode,
                     Tracer *tracer,
                     const std::shared_ptr<google::protobuf::Arena> &arena);

    void sendError(anet::Connection *pConnection,
                   channelid_t channelId,
                   version_t requestVersion,
                   const std::string &errMsg,
                   ErrorCode errorCode,
                   Tracer *tracer,
                   const std::shared_ptr<google::protobuf::Arena> &arena);

    anet::IPacketHandler::HPRetCode handleCmdPacket(anet::Packet *pPacket);

    ErrorCode processRequest(CodecContext *pContext,
                             anet::Connection *pConnection,
                             channelid_t channelId,
                             version_t requestVersion,
                             Tracer *tracer);

    ErrorCode decodeRequestPacket(anet::DelayDecodePacket *packet,
                                  RPCService *&service,
                                  RPCMethodDescriptor *&method,
                                  RPCMessage *&request);

    anet::Packet *encodeErrorResponse(channelid_t channelId,
                                      version_t requestVersion,
                                      const std::string &errMsg,
                                      ErrorCode errorCode,
                                      Tracer *tracer,
                                      const std::shared_ptr<google::protobuf::Arena> &arena);
    std::string getClientIpStr(anet::Connection *conn);

    ErrorCode decodePacket(anet::Packet *packet, CodecContext *&context);
    anet::Packet *encodeResponse(RPCMessage *response,
                                 Tracer *tracer,
                                 version_t version,
                                 const std::shared_ptr<google::protobuf::Arena> &arena);

    friend class ANetRPCServerAdapterTest;

protected:
    ANetRPCMessageCodec *_messageCodec;
    std::shared_ptr<ServerMetricReporter> _metricReporter;
};

ARPC_END_NAMESPACE(arpc)