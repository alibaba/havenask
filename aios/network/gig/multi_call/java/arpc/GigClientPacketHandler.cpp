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
#include "aios/network/gig/multi_call/java/arpc/GigClientPacketHandler.h"
#include "aios/autil/autil/Lock.h"
#include "aios/network/arpc/arpc/MessageSerializable.h"
#include "aios/network/arpc/arpc/RPCMessageSerializable.h"
#include "aios/network/gig/multi_call/java/arpc/GigRPCChannel.h"
#include "aios/network/gig/multi_call/java/arpc/GigRPCMessageSerializable.h"

using namespace std;
using namespace arpc;
using namespace anet;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigClientPacketHandler);

GigClientPacketHandler::GigClientPacketHandler() { _ref = 0; }

GigClientPacketHandler::~GigClientPacketHandler() {}

anet::IPacketHandler::HPRetCode
GigClientPacketHandler::handlePacket(anet::Packet *packet, void *args) {

    AUTIL_LOG(DEBUG, "handle package in gig client package handler");
    IPacketHandler::HPRetCode ret = doHandlePacket(packet, args);
    subRef();
    return ret;
}

IPacketHandler::HPRetCode GigClientPacketHandler::doHandlePacket(Packet *packet,
                                                                 void *args) {
    GigRpcReqArg *pArgs = (GigRpcReqArg *)args;
    if (!packet->isRegularPacket()) {
        AUTIL_LOG(WARN, "receive control packet[%d]",
                  ((ControlPacket *)packet)->getCommand());
        return handleCmdPacket(packet, pArgs);
    }

    pArgs->sController->GetTracer().BeginHandleResponse();

    AUTIL_LOG(TRACE1, "channel pointer: %p, msg chid: %lu", _gigRpcChannel,
              packet->getChannelId());
    {
        /* Check the error code and remote version.
         * Repost the request if necessary.
         */
        autil::ScopedLock lock(_channelLock);
        if (_gigRpcChannel &&
            !_gigRpcChannel->CheckResponsePacket(packet, pArgs)) {
            return IPacketHandler::FREE_CHANNEL;
        }
    }

    decodePacket(pArgs->sController, packet, pArgs->sResponse,
                 pArgs->sContext->arena);
    pArgs->sClosure->Run();
    delete pArgs;

    return IPacketHandler::FREE_CHANNEL;
}

void GigClientPacketHandler::decodePacket(
    ANetRPCController *controller, Packet *packet, std::string *response,
    const std::shared_ptr<google::protobuf::Arena> &arenaPtr) {
    TraceInfo *serverTraceInfo = NULL;
    const Tracer &tracer = controller->GetTracer();

    if (tracer.GetTraceFlag()) {
        serverTraceInfo =
            google::protobuf::Arena::CreateMessage<TraceInfo>(arenaPtr.get());
    }

    version_t version = packet->getPacketVersion();
    int32_t pcode = packet->getPcode();
    DataBufferSerializable *pSeri =
        createSerializable(pcode, response, serverTraceInfo, version, arenaPtr);

    if (pSeri == NULL) {
        controller->SetErrorCode(ARPC_ERROR_NEW_NOTHROW);
        controller->SetFailed("create MessageSerializable failed.");
        packet->free();
        return;
    }

    DelayDecodePacket *delayDecodePacket =
        dynamic_cast<DelayDecodePacket *>(packet);
    assert(delayDecodePacket);
    delayDecodePacket->setContent(pSeri, true);

    bool ret = delayDecodePacket->decodeToContent();

    if (!ret) {
        controller->SetErrorCode(ARPC_ERROR_DECODE_PACKET);
        controller->SetFailed(errorCodeToString(ARPC_ERROR_DECODE_PACKET));
    } else {
        if (pcode == ARPC_ERROR_NO_RESPONSE_SENT) {
            /* This is a special error message sent from server, which means the
             * server does not intentionally send back any message when
             * processing this request. In this case, we will log a warning
             * message in client side. We do not encourage such case. */
            AUTIL_LOG(WARN,
                      "Server does not respond to msg chid %lu, which is"
                      " not normal case. Error msg: %s",
                      packet->getChannelId(),
                      errorCodeToString(ARPC_ERROR_NO_RESPONSE_SENT).c_str());
        }

        if (pcode != ARPC_ERROR_NONE) {

            ErrorMsg *errMsg = NULL;

            if (version == ARPC_VERSION_1 && tracer.GetTraceFlag()) {
                RPCMessageSerializable *serializable =
                    dynamic_cast<RPCMessageSerializable *>(pSeri);
                assert(serializable);
                errMsg = (ErrorMsg *)serializable->getBody();
            } else {
                MessageSerializable *serializable =
                    dynamic_cast<MessageSerializable *>(pSeri);
                assert(serializable);
                errMsg = (ErrorMsg *)serializable->getMessage();
            }

            controller->SetFailed(errMsg->error_msg());
            if (errMsg->error_code())
                pcode = errMsg->error_code();
            controller->SetErrorCode((ErrorCode)pcode);
        }
    }

    if (serverTraceInfo != NULL) {
        TraceInfo *clientTraceInfo =
            tracer.ExtractClientTraceInfo(arenaPtr.get());
        serverTraceInfo->MergeFrom(*clientTraceInfo);
        controller->SetTraceInfo(serverTraceInfo);
    }

    delayDecodePacket->free();
    return;
}

DataBufferSerializable *GigClientPacketHandler::createSerializable(
    int32_t pcode, std::string *response, TraceInfo *traceInfo,
    version_t version,
    const std::shared_ptr<google::protobuf::Arena> &arenaPtr) {
    DataBufferSerializable *pSeri = NULL;

    if (version == ARPC_VERSION_1 && traceInfo != NULL) {
        if (pcode != ARPC_ERROR_NONE) {
            ErrorMsg *errMsg = google::protobuf::Arena::CreateMessage<ErrorMsg>(
                arenaPtr.get());

            if (errMsg == NULL) {
                return NULL;
            }

            pSeri = new (nothrow)
                RPCMessageSerializable(traceInfo, errMsg, arenaPtr);
        } else {
            pSeri =
                new (nothrow) GigRPCMessageSerializable(traceInfo, response);
        }
    }

    return pSeri;
}

IPacketHandler::HPRetCode
GigClientPacketHandler::handleCmdPacket(Packet *packet, GigRpcReqArg *pArgs) {
    ErrorCode errCode = ARPC_ERROR_NONE;

    switch (((ControlPacket *)packet)->getCommand()) {
    case ControlPacket::CMD_BAD_PACKET:
        errCode = ARPC_ERROR_BAD_PACKET;
        break;

    case ControlPacket::CMD_TIMEOUT_PACKET:
        errCode = ARPC_ERROR_TIMEOUT;
        break;

    case ControlPacket::CMD_CONNECTION_CLOSED:
        errCode = ARPC_ERROR_CONNECTION_CLOSED;
        break;

    default:
        errCode = ARPC_ERROR_UNKNOWN_PACKET;
        break;
    }

    pArgs->sController->SetErrorCode(errCode);
    pArgs->sController->SetFailed(errorCodeToString(errCode));
    pArgs->sClosure->Run();
    delete pArgs;

    return IPacketHandler::FREE_CHANNEL;
}

void GigClientPacketHandler::cleanChannel() {
    autil::ScopedLock lock(_channelLock);
    setGigRpcChannel(NULL);
}

void GigClientPacketHandler::addRef() {
    autil::ScopedLock lock(_mutex);
    _ref++;
    AUTIL_LOG(DEBUG, "add ref, ref count:%ld.", _ref);
}

void GigClientPacketHandler::subRef() {
    int64_t ref = 0;
    {
        autil::ScopedLock lock(_mutex);
        ref = --_ref;
    }

    AUTIL_LOG(DEBUG, "sub ref, ref count:%ld.", ref);
    if (ref == 0) {
        delete this;
    }
}

} // namespace multi_call
