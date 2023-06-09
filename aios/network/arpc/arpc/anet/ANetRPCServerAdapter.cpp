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
#include "aios/network/arpc/arpc/anet/ANetRPCServerAdapter.h"
#include "aios/network/anet/ioworker.h"
#include "aios/network/anet/addrspec.h"
#include "aios/network/anet/atomic.h"
#include "aios/network/anet/connection.h"
#include "aios/network/anet/connectionpriority.h"
#include "aios/network/anet/controlpacket.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/iocomponent.h"
#include "aios/network/anet/ipackethandler.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/socket.h"
#include "aios/network/anet/delaydecodepacket.h"
#include "aios/network/arpc/arpc/anet/ANetRPCServerWorkItem.h"
#include "aios/network/arpc/arpc/PacketArg.h"
#include "aios/network/arpc/arpc/RPCServer.h"
#include "aios/network/arpc/arpc/UtilFun.h"

using namespace anet;
using namespace std;
ARPC_BEGIN_NAMESPACE(arpc);

ANetRPCServerAdapter::ANetRPCServerAdapter(RPCServer *pRpcServer,
                                          ANetRPCMessageCodec *messageCodec)
    : RPCServerAdapter(pRpcServer)
    , _messageCodec(messageCodec) {
}

ANetRPCServerAdapter::~ANetRPCServerAdapter() {
}

anet::IPacketHandler::HPRetCode ANetRPCServerAdapter::handlePacket(anet::Connection *pConnection,
                                                                   anet::Packet *pPacket) {
    if (!pPacket->isRegularPacket()) {
        return handleCmdPacket(pPacket);
    }
    Tracer *tracer = new Tracer();
    tracer->SetBeginHandleRequest(IoWorker::_loopTime);

    DelayDecodePacket *pReqPacket = (DelayDecodePacket *)pPacket;
    CodecContext *pContext = NULL;

    ErrorCode ret = decodePacket(pPacket, pContext);

    std::shared_ptr<google::protobuf::Arena> arena;
    if (pContext) {
        arena = pContext->arena;
    }
    if (!arena) {
        arena.reset(new google::protobuf::Arena());
    }
    if (ARPC_ERROR_NONE != ret) {
        handleError(pConnection, pReqPacket, ret, tracer, arena);
        pReqPacket->free();
        delete pContext;
        return anet::IPacketHandler::KEEP_CHANNEL;
    }

    tracer->SetTraceFlag(pContext->enableTrace);
    tracer->EndHandleRequest();
    tracer->SetClientTimeout(pContext->timeout);

    assert(pContext);
    ret = processRequest(pContext, pConnection,
                         pReqPacket->getChannelId(),
                         pReqPacket->getPacketVersion(),
                         tracer);

    if (ARPC_ERROR_NONE != ret) {
        handleError(pConnection, pReqPacket, ret, tracer, arena);
        pReqPacket->free();
        delete pContext;
        return IPacketHandler::KEEP_CHANNEL;
    }

    pReqPacket->free();
    delete pContext;
    return IPacketHandler::KEEP_CHANNEL;
}

ErrorCode ANetRPCServerAdapter::processRequest(CodecContext *pContext,
        Connection *pConnection, channelid_t channelId,
        version_t requestVersion, Tracer *tracer)
{
    if(pContext->qosId != pConnection->getQosGroup())
        pConnection->setQosGroup(0, 0, pContext->qosId);
        
    RPCServerWorkItem *pWorkItem = new ANetRPCServerWorkItem(pContext->rpcService, pContext->rpcMethodDes,
            pContext->request, pContext->userPayload, pConnection, channelId, _messageCodec,
            requestVersion, tracer);
    auto errCode = doPushWorkItem(pWorkItem, pContext, tracer);
    if (errCode != ARPC_ERROR_NONE) {
        ARPC_LOG(ERROR, "do push request work item failed, channel id %ld", channelId);
    }
    return errCode;
}

IPacketHandler::HPRetCode ANetRPCServerAdapter::handleCmdPacket(Packet *pPacket)
{
    ControlPacket *controlPacket = (ControlPacket *)pPacket;

    if (controlPacket->getCommand()
            == ControlPacket::CMD_RECEIVE_NEW_CONNECTION) {
        atomic_inc(&_clientConnNum);
    } else if (controlPacket->getCommand()
               == ControlPacket::CMD_CLOSE_CONNECTION) {
        atomic_dec(&_clientConnNum);
    } else {
        ARPC_LOG(ERROR, "get cmd packet cmd[%d]",
                 controlPacket->getCommand());
    }

    return anet::IPacketHandler::FREE_CHANNEL;
}

void ANetRPCServerAdapter::handleError(Connection *pConnection,
                                       DelayDecodePacket *pReqPacket,
                                       ErrorCode errCode,
                                       Tracer *tracer,
                                       const std::shared_ptr<google::protobuf::Arena> &arena)
{
    // TODO: return error
    uint32_t rpcCode = (uint32_t)(pReqPacket->getPcode());
    uint16_t serviceId = 0;
    uint16_t methodId = 0;
    PacketCodeParser()(rpcCode, serviceId, methodId);
    const std::string &errMsg = errorCodeToString(errCode);
    ARPC_LOG(ERROR,
             "rpccall error:serviceId[%ud], methodId[%ud], "
             "errorCode[%d], errorMsg[%s]",
             serviceId,
             methodId,
             errCode,
             errMsg.c_str());

    sendError(pConnection, pReqPacket->getChannelId(),
              pReqPacket->getPacketVersion(), errMsg, errCode,
              tracer, arena);
    delete tracer;
}

void ANetRPCServerAdapter::sendError(Connection *pConnection,
                                     channelid_t channelId,
                                     version_t requestVersion,
                                     const string &errMsg,
                                     ErrorCode errorCode,
                                     Tracer *tracer,
                                     const std::shared_ptr<google::protobuf::Arena> &arena)
{
    Packet *packet = encodeErrorResponse(channelId, requestVersion,
            errMsg, errorCode, tracer, arena);

    if (packet == NULL) {
        return;
    }

    if (!pConnection->postPacket(packet)) {
        delete packet;
        ARPC_LOG(ERROR, "post packet error");
    }
}

Packet *ANetRPCServerAdapter::encodeErrorResponse(channelid_t channelId,
        version_t requestVersion, const string &errMsg,
        ErrorCode errorCode, Tracer *tracer,
        const std::shared_ptr<google::protobuf::Arena> &arena)
{
    version_t version = requestVersion;

    if (errorCode == ARPC_ERROR_INVALID_VERSION) {
        version = _pRpcServer->GetVersion();
    }

    ErrorMsg *pErrMsg = BuildErrorMsg(errMsg, errorCode, arena);

    if (pErrMsg == NULL) {
        ARPC_LOG(ERROR, "build error msg return NULL");
        return NULL;
    }

    Packet *packet = encodeResponse(pErrMsg, tracer, version, arena);

    if (packet == NULL) {
        return NULL;
    }

    int32_t pcode = packet->getPcode();
    pcode |= errorCode;
    packet->setPcode(pcode);
    packet->setChannelId(channelId);

    return packet;
}

string ANetRPCServerAdapter::getClientIpStr(anet::Connection *conn) {
    string clientAddress;
    anet::IOComponent* ioComponent = NULL;
    if (NULL != (ioComponent = conn->getIOComponent()))
    {
        anet::Socket* socket = ioComponent->getSocket();
        if (socket) {
            char dest[30];
            memset(dest, 0, sizeof(dest[0]) * 30);
            if (socket->getAddr(dest, 30, false) != NULL) {
                clientAddress.assign(dest);
                return clientAddress;
            }
        }
    }
    clientAddress.assign("unknownAddress");
    return clientAddress;
}

ErrorCode ANetRPCServerAdapter::decodePacket(anet::Packet *packet,
                                             CodecContext *&context) {
    version_t version = packet->getPacketVersion();
    context = _messageCodec->DecodeRequest(packet, _pRpcServer, version);
    ErrorCode ec = context->errorCode;
    return ec;
}

anet::Packet *ANetRPCServerAdapter::encodeResponse(RPCMessage *response,
                                                   Tracer *tracer,
                                                   version_t version,
                                                   const std::shared_ptr<google::protobuf::Arena> &arena) {
    return _messageCodec->EncodeResponse(response, tracer, version, arena);
}

ARPC_END_NAMESPACE(arpc)