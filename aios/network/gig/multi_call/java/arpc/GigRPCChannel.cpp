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
#include "aios/network/gig/multi_call/java/arpc/GigRPCChannel.h"
#include "aios/network/arpc/arpc/SyncClosure.h"
#include "aios/network/gig/multi_call/java/arpc/GigRPCArg.h"

using namespace std;
using namespace arpc;
using namespace anet;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigRPCChannel);

GigRPCChannel::GigRPCChannel(anet::Connection *pConnection,
                             GigRPCMessageCodec *gigMessageCodec, bool block)
    : ANetRPCChannel(pConnection, gigMessageCodec, block),
      _gigMessageCodec(gigMessageCodec),
      _gigClientPacketHandler(new GigClientPacketHandler()) {
    _gigClientPacketHandler->setGigRpcChannel(this);
    _gigClientPacketHandler->addRef();
    // clear default package handler
    SharedClientPacketHandler *sharedHandler = getSharedHandlerFromConnection();
    if (sharedHandler != NULL) {
        if (_pConnection) {
            _pConnection->setDefaultPacketHandler(NULL);
        }
        sharedHandler->cleanChannel();
        sharedHandler->subRef();
    }
}

GigRPCChannel::~GigRPCChannel() {
    if (_pConnection) {
        _pConnection->close(); // should close here but donot subRef
        if (_gigClientPacketHandler) {
            _gigClientPacketHandler->cleanChannel();
            _gigClientPacketHandler->subRef();
        }
    }
}

void GigRPCChannel::CallMethod(uint32_t serviceId, uint32_t methodId,
                               RPCController *controller,
                               const std::string *request,
                               std::string *response, RPCClosure *done) {
    version_t version = GetVersion();
    ANetRPCController *pController = (ANetRPCController *)controller;
    SetTraceFlag(pController);

    uint32_t pcode = (serviceId << 16) | methodId;

    GigCodecContext *context = new GigCodecContext();
    context->callId.intId = pcode;
    context->request = (std::string *)request;
    context->enableTrace = pController->GetTraceFlag();
    context->userPayload = pController->GetTracer().getUserPayload();
    pController->setProtoArena(context->arena);

    Packet *packet = _gigMessageCodec->EncodeRequest(context, pcode, version);

    if (packet == NULL) {
        SetError(pController, ARPC_ERROR_ENCODE_PACKET);
        RunClosure(done);
        delete context;
        return;
    }

    GigRpcReqArg *pArg =
        new (nothrow) GigRpcReqArg(pController, response, done, context);

    if (pArg == NULL) {
        AUTIL_LOG(ERROR, "new GigRpcReqArg return NULL");
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

void GigRPCChannel::SyncCall(Packet *pPack, GigRpcReqArg *pArg) {
    SyncClosure syncDone;
    pArg->sClosure = &syncDone;
    AsyncCall(pPack, pArg);
    syncDone.WaitReply();
}

bool GigRPCChannel::AsyncCall(Packet *pPack, GigRpcReqArg *pArg) {
    ANetRPCController *pController = pArg->sController;
    pController->GetTracer().BeginPostRequest();
    pPack->setExpireTime(pController->GetExpireTime());
    pArg->sVersion = pPack->getPacketVersion();
    if (_gigClientPacketHandler) {
        _gigClientPacketHandler->addRef();
    }
    bool ret = PostPacket(pPack, _gigClientPacketHandler, pArg, _block);

    if (!ret) {
        AUTIL_LOG(WARN, "post packet error");
        pPack->free();
        if (_gigClientPacketHandler) {
            _gigClientPacketHandler->subRef();
        }

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

void GigRPCChannel::SetError(ANetRPCController *pController,
                             ErrorCode errorCode) {
    pController->SetErrorCode(errorCode);
    pController->SetFailed(errorCodeToString(errorCode));
    return;
}

void GigRPCChannel::RunClosure(RPCClosure *pClosure) {
    if (pClosure != NULL) {
        pClosure->Run();
    }
}

bool GigRPCChannel::CheckResponsePacket(Packet *packet, GigRpcReqArg *pArgs) {
    uint32_t pcode = packet->getPcode();
    ErrorCode errorCode = (ErrorCode)(pcode & (~ADVANCE_PACKET_MASK));
    version_t remoteVersion = packet->getPacketVersion();

    AUTIL_LOG(TRACE1,
              "check response packet, pcode [%d], errorcode [%d], "
              "remoteversion [%d]",
              pcode, errorCode, remoteVersion);

    if (!needRepostPacket(errorCode, remoteVersion, pArgs->sVersion)) {
        return true;
    }

    SetVersion(remoteVersion);
    GigCodecContext *context = pArgs->sContext;
    Packet *newPacket =
        _gigMessageCodec->EncodeRequest(context, pcode, remoteVersion);

    if (newPacket == NULL) {
        AUTIL_LOG(ERROR, "encode repost packet falied. packetVersion [%d]",
                  remoteVersion);
        return true;
    }

    delete packet;
    AsyncCall(newPacket, pArgs);
    AUTIL_LOG(INFO, "repost the request for remote version [%d]",
              remoteVersion);
    return false;
}

bool GigRPCChannel::needRepostPacket(ErrorCode errorCode,
                                     version_t remoteVersion,
                                     version_t postedPacketVersion) const {
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

} // namespace multi_call
