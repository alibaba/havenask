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
#ifndef ISEARCH_MULTI_CALL_GIGCLIENTPACKETHANDLER_H
#define ISEARCH_MULTI_CALL_GIGCLIENTPACKETHANDLER_H

#include "aios/autil/autil/Lock.h"
#include "aios/network/arpc/arpc/anet/ClientPacketHandler.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/java/arpc/GigRPCArg.h"

namespace multi_call {

class GigRPCChannel;

class GigClientPacketHandler : public arpc::ClientPacketHandler {
public:
    GigClientPacketHandler();
    ~GigClientPacketHandler();

private:
    GigClientPacketHandler(const GigClientPacketHandler &);
    GigClientPacketHandler &operator=(const GigClientPacketHandler &);

public:
    anet::IPacketHandler::HPRetCode handlePacket(anet::Packet *packet,
                                                 void *args);
    void addRef();
    void subRef();
    void cleanChannel();
    void setGigRpcChannel(GigRPCChannel *channel) { _gigRpcChannel = channel; }

private:
    IPacketHandler::HPRetCode doHandlePacket(anet::Packet *packet, void *args);
    IPacketHandler::HPRetCode handleCmdPacket(anet::Packet *packet,
                                              GigRpcReqArg *pArgs);
    void decodePacket(arpc::ANetRPCController *controller, anet::Packet *packet,
                      std::string *response,
                      const std::shared_ptr<google::protobuf::Arena> &arenaPtr);
    anet::DataBufferSerializable *createSerializable(
        int32_t pcode, std::string *response, arpc::TraceInfo *traceInfo,
        version_t version,
        const std::shared_ptr<google::protobuf::Arena> &arenaPtr);

private:
    GigRPCChannel *_gigRpcChannel;
    int64_t _ref;
    mutable autil::ThreadMutex _mutex;
    mutable autil::ThreadMutex _channelLock;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigClientPacketHandler);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGCLIENTPACKETHANDLER_H
