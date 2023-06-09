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
#ifndef ARPC_ANET_RPC_CHANNEL_H
#define ARPC_ANET_RPC_CHANNEL_H

#include "aios/network/anet/anet.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/arpc/arpc/RPCChannelBase.h"
#include "aios/network/arpc/arpc/anet/ClientPacketHandler.h"
#include "aios/network/arpc/arpc/anet/SharedClientPacketHandler.h"
#include "aios/network/arpc/arpc/anet/ANetRPCMessageCodec.h"

ARPC_BEGIN_NAMESPACE(arpc);

class ANetRPCChannel : public RPCChannelBase
{
public:
    ANetRPCChannel(anet::Connection *pConnection,
                   ANetRPCMessageCodec *messageCode,
                   bool block = false);
    virtual ~ANetRPCChannel();
public:
    virtual void CallMethod(const RPCMethodDescriptor *method,
                            RPCController *controller,
                            const RPCMessage *request,
                            RPCMessage *response,
                            RPCClosure *done) override;
    virtual bool ChannelBroken() override;
    virtual bool ChannelConnected() override;
    bool CheckResponsePacket(anet::Packet *packet, RpcReqArg *pArgs);
    std::string getRemoteAddr() const;

public: // for test
    void SyncCall(anet::Packet *pPack,  RpcReqArg *pArg);
    bool AsyncCall(anet::Packet *pPack,  RpcReqArg *pArg);
    SharedClientPacketHandler *getSharedHandlerFromConnection();

protected:
    virtual bool PostPacket(anet::Packet *packet,
                            anet::IPacketHandler *packetHandler,
                            void *args, bool block);

    bool needRepostPacket(ErrorCode errorCode,
                          version_t remoteVersion,
                          version_t postedPacketVersion) const;

protected:
    ClientPacketHandler _handler;
    anet::Connection *_pConnection;
    bool _block;
    ANetRPCMessageCodec *_messageCodec;
private:
    friend class ANetRPCChannelTest;
};

ARPC_END_NAMESPACE(arpc);
#endif //ARPC_ANET_RPC_CHANNEL_H
