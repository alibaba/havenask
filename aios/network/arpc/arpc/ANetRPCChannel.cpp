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
#include <google/protobuf/arena.h>
#include <new>
#include <cassert>
#include <cstddef>
#include <memory>
#include <string>

#include "aios/network/arpc/arpc/util/Log.h"
#include "aios/network/anet/delaydecodepacket.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/arpc/arpc/MessageSerializable.h"
#include "aios/network/arpc/arpc/PacketArg.h"
#include "aios/network/arpc/arpc/ANetRPCChannel.h"
#include "aios/network/arpc/arpc/UtilFun.h"
#include "aios/network/anet/connectionpriority.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/packet.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/arpc/arpc/anet/ClientPacketHandler.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/RPCChannelBase.h"
#include "aios/network/arpc/arpc/Tracer.h"
#include "aios/network/arpc/arpc/SyncClosure.h"

using namespace std;
using namespace anet;
ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(ANetRPCChannel);

ANetRPCChannel::ANetRPCChannel(Connection *pConnection,
                               ANetRPCMessageCodec *messageCodec,
                               bool block)
{
    _pConnection = pConnection;

    if (_pConnection) {
        SharedClientPacketHandler *sharedHandler = new SharedClientPacketHandler;
        sharedHandler->setChannel(this);
        _pConnection->setDefaultPacketHandler(sharedHandler);
        sharedHandler->addRef();
    }

    _block = block;
    _messageCodec = messageCodec;
    _handler.setChannel(this);
}

ANetRPCChannel::~ANetRPCChannel() {
    if (_pConnection) {
        _pConnection->close();
        SharedClientPacketHandler *sharedHandler =
            getSharedHandlerFromConnection();
        if (sharedHandler != NULL) {
            sharedHandler->cleanChannel();
            sharedHandler->subRef();
        }
        _pConnection->subRef();
    }

    delete _messageCodec;
}

void ANetRPCChannel::CallMethod(const RPCMethodDescriptor *method,
                                RPCController *controller,
                                const RPCMessage *request,
                                RPCMessage *response,
                                RPCClosure *done)
{
    version_t version = GetVersion();
    ANetRPCController *pController = (ANetRPCController *)controller;
    SetTraceFlag(pController);

    CodecContext *context = new CodecContext;
    context->callId = _messageCodec->GenerateCallId(method, version);
    context->request = (RPCMessage *)request;
    context->enableTrace = pController->GetTraceFlag();
    context->userPayload = pController->GetTracer().getUserPayload();
    if (!response->GetArena()) {
        context->arena.reset(new google::protobuf::Arena());
        pController->setProtoArena(context->arena);
    }

    Packet *packet = _messageCodec->EncodeRequest(context, version);

    if (packet == NULL) {
        SetError(pController, ARPC_ERROR_ENCODE_PACKET);
        RunClosure(done);
        delete context;
        return;
    }

    RpcReqArg *pArg =
        new (nothrow) RpcReqArg(pController, response, done, context);

    if (pArg == NULL) {
        ARPC_LOG(ERROR, "new RpcReqArg return NULL");
        SetError(pController, ARPC_ERROR_NEW_NOTHROW);
        RunClosure(done);
        delete context;
        delete packet;
        return;
    }

    if (done == NULL) {
        SyncCall(packet, pArg);
    } else {
        AsyncCall(packet, pArg);
    }
}

void ANetRPCChannel::SyncCall(Packet *pPack,  RpcReqArg *pArg)
{
    SyncClosure syncDone;
    pArg->sClosure = &syncDone;
    AsyncCall(pPack, pArg);
    syncDone.WaitReply();
}

bool ANetRPCChannel::AsyncCall(Packet *pPack, RpcReqArg *pArg)
{
    ANetRPCController *pController = pArg->sController;
    pController->GetTracer().BeginPostRequest();
    pPack->setExpireTime(pController->GetExpireTime());
    pArg->sVersion = pPack->getPacketVersion();
    bool ret = PostPacket(pPack, NULL, pArg, _block);

    if (!ret) {
        ARPC_LOG(WARN, "post packet error");
        pPack->free();

        if (_pConnection->isClosed()) {
            SetError(pController, ARPC_ERROR_CONNECTION_CLOSED);
        } else {
            SetError(pController, ARPC_ERROR_POST_PACKET);
        }

        RunClosure(pArg->sClosure);
        delete pArg;
        pArg = NULL;
        return false;
    }

    return true;
}

SharedClientPacketHandler* ANetRPCChannel::getSharedHandlerFromConnection() {
    if (_pConnection == NULL) {
        return NULL;
    }
    return dynamic_cast<SharedClientPacketHandler*> (
            _pConnection->getDefaultPacketHandler());
}

bool ANetRPCChannel::needRepostPacket(
    ErrorCode errorCode, version_t remoteVersion,
    version_t postedPacketVersion) const
{
    if (errorCode == ARPC_ERROR_NONE) {
        return false;
    }

    if (errorCode == ARPC_ERROR_RPCCALL_MISMATCH) {
        if (remoteVersion == ARPC_VERSION_0 &&
                postedPacketVersion > ARPC_VERSION_0) {
            return true;
        } else {
            return false;
        }
    }

    if (errorCode == ARPC_ERROR_INVALID_VERSION) {
        if (remoteVersion < postedPacketVersion) {
            return true;
        }

        return false;
    }

    return false;
}

bool ANetRPCChannel::CheckResponsePacket(Packet *packet, RpcReqArg *pArgs)
{
    uint32_t pcode = packet->getPcode();
    ErrorCode errorCode = (ErrorCode)(pcode & (~ADVANCE_PACKET_MASK));
    version_t remoteVersion = packet->getPacketVersion();

    ARPC_LOG(TRACE1, "check response packet, pcode [%d], errorcode [%d], "
             "remoteversion [%d]", pcode, errorCode, remoteVersion);

    if (!needRepostPacket(errorCode, remoteVersion, pArgs->sVersion)) {
        return true;
    }

    SetVersion(remoteVersion);
    CodecContext *context = pArgs->sContext;
    Packet *newPacket = _messageCodec->EncodeRequest(context, remoteVersion);

    if (newPacket == NULL) {
        ARPC_LOG(ERROR, "encode repost packet falied. packetVersion [%d]",
                 remoteVersion);
        return true;
    }

    delete packet;
    AsyncCall(newPacket, pArgs);
    ARPC_LOG(INFO, "repost the request for remote version [%d]", remoteVersion);
    return false;
}

bool ANetRPCChannel::PostPacket(Packet *packet,
                                IPacketHandler *packetHandler,
                                void *args, bool block)
{
    if (_pConnection) {
        SharedClientPacketHandler *sharedHandler =
            getSharedHandlerFromConnection();
        if (sharedHandler != NULL) {
            sharedHandler->addRef();
        }
        bool ret =  _pConnection->postPacket(packet, 
                                        packetHandler, args, block);
        if (!ret) {
            if (sharedHandler) {
                sharedHandler->subRef();
            }
        }
        return ret;
    }

    return false;
}


bool ANetRPCChannel::ChannelConnected()
{
    if (_pConnection) {
        return _pConnection->isConnected();
    }

    return false;
}

bool ANetRPCChannel::ChannelBroken()
{
    if (_pConnection) {
        return _pConnection->isClosed();
    }

    return false;
}

std::string ANetRPCChannel::getRemoteAddr() const {
    if (_pConnection) {
        std::string addr;
        addr.resize(32);
        _pConnection->getIpAndPortAddr(addr.data(), addr.size());
        return std::string(addr.data());
    }
    return std::string();
}

ARPC_END_NAMESPACE(arpc);
