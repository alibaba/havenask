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
#include <assert.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/descriptor.h>
#include <stdint.h>
#include <cstddef>
#include <memory>
#include <new>
#include <string>

#include "aios/network/arpc/arpc/util/Log.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "aios/network/arpc/arpc/anet/ClientPacketHandler.h"
#include "aios/network/arpc/arpc/PacketArg.h"
#include "aios/network/arpc/arpc/MessageSerializable.h"
#include "aios/network/arpc/arpc/RPCMessageSerializable.h"
#include "aios/network/arpc/arpc/RPCChannelBase.h"
#include "aios/network/arpc/arpc/UtilFun.h"
#include "aios/network/anet/controlpacket.h"
#include "aios/network/anet/databufferserializable.h"
#include "aios/network/anet/delaydecodepacket.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/ipackethandler.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/runnable.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/MessageCodec.h"
#include "aios/network/arpc/arpc/Tracer.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/arpc/arpc/ANetRPCChannel.h"

using namespace std;
using namespace anet;
using namespace google::protobuf::io;
ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(ClientPacketHandler);

ClientPacketHandler::ClientPacketHandler()
{
    _channel = NULL;
}

ClientPacketHandler::~ClientPacketHandler()
{
    _channel = NULL;
}

IPacketHandler::HPRetCode
ClientPacketHandler::handlePacket(anet::Packet *packet, void *args)
{
    RpcReqArg *pArgs = (RpcReqArg *)args;

    if (!packet->isRegularPacket()) {
        ARPC_LOG(WARN, "receive control packet[%d]",
                 ((ControlPacket *)packet)->getCommand());
        return handleCmdPacket(packet, pArgs);
    }

    pArgs->sController->GetTracer().BeginHandleResponse();

    ARPC_LOG(TRACE1, "channel pointer: %p, msg chid: %lu", _channel,
             packet->getChannelId());

    /* Check the error code and remote version.
     * Repost the request if necessary.
     */
    if (_channel && !_channel->CheckResponsePacket(packet, pArgs)) {
        return IPacketHandler::FREE_CHANNEL;
    }

    decodePacket(pArgs->sController, packet, pArgs->sResponse,
                 pArgs->sContext->arena);
    pArgs->sClosure->Run();
    delete pArgs;

    return IPacketHandler::FREE_CHANNEL;
}

IPacketHandler::HPRetCode
ClientPacketHandler::handleCmdPacket(Packet *packet, RpcReqArg *pArgs)
{
    ErrorCode errCode = ARPC_ERROR_NONE;

    switch(((ControlPacket *)packet)->getCommand()) {
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

void ClientPacketHandler::decodePacket(ANetRPCController *controller,
                                       Packet *packet, RPCMessage *response,
                                       const std::shared_ptr<google::protobuf::Arena> &arenaPtr)
{
    TraceInfo *serverTraceInfo = NULL;
    const Tracer &tracer = controller->GetTracer();

    auto arena = getArena(response, arenaPtr);
    assert(arena);
    if (tracer.GetTraceFlag()) {
        serverTraceInfo = google::protobuf::Arena::CreateMessage<TraceInfo>(arena);
    }

    version_t version = packet->getPacketVersion();
    int32_t pcode = packet->getPcode();
    DataBufferSerializable *pSeri = createSerializable(pcode, response,
            serverTraceInfo, version, arena, arenaPtr);

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
             * server does not intentionally send back any message when processing
             * this request.
             * In this case, we will log a warning message in client side. We do
             * not encourage such case. */
            ARPC_LOG(WARN, "Server does not respond to msg chid %lu, which is"
                     " not normal case. Error msg: %s", packet->getChannelId(),
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
            if (errMsg->error_code()) pcode = errMsg->error_code();
            controller->SetErrorCode((ErrorCode)pcode);
        }
    }

    if (serverTraceInfo != NULL) {
        TraceInfo *clientTraceInfo = tracer.ExtractClientTraceInfo(arena);
        serverTraceInfo->MergeFrom(*clientTraceInfo);
        controller->SetTraceInfo(serverTraceInfo);
    }

    delayDecodePacket->free();
    return;
}

DataBufferSerializable *ClientPacketHandler::createSerializable(int32_t pcode,
        RPCMessage *response, TraceInfo *traceInfo, version_t version,
        google::protobuf::Arena *arena,
        const std::shared_ptr<google::protobuf::Arena> &arenaPtr)
{
    DataBufferSerializable *pSeri = NULL;

    if (version == ARPC_VERSION_1 && traceInfo != NULL) {
        if (pcode != ARPC_ERROR_NONE) {
            ErrorMsg *errMsg = google::protobuf::Arena::CreateMessage<ErrorMsg>(arena);

            if (errMsg == NULL) {
                return NULL;
            }

            pSeri = new (nothrow) RPCMessageSerializable(traceInfo, errMsg, arenaPtr);
        } else {
            pSeri = new (nothrow) RPCMessageSerializable(traceInfo, response, arenaPtr);
        }
    } else {
        if (pcode != ARPC_ERROR_NONE) {
            ErrorMsg *errMsg = google::protobuf::Arena::CreateMessage<ErrorMsg>(arena);

            if (errMsg == NULL) {
                return NULL;
            }

            pSeri = new (nothrow) MessageSerializable(errMsg, arenaPtr);
        } else {
            pSeri = new (nothrow) MessageSerializable(response, arenaPtr);
        }
    }

    return pSeri;
}

void ClientPacketHandler::repostPacket(anet::Packet *packet, void *args)
{

    assert(false);
}

RPCChannelBase *ClientPacketHandler::getChannel() {
    return _channel;
}

ARPC_END_NAMESPACE(arpc);

