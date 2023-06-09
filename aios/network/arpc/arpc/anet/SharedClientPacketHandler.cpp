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
#include "aios/network/arpc/arpc/anet/SharedClientPacketHandler.h"

#include <stddef.h>

#include "aios/network/arpc/arpc/RPCChannelBase.h"
#include "aios/network/anet/controlpacket.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/runnable.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/arpc/arpc/MessageCodec.h"
#include "aios/network/arpc/arpc/PacketArg.h"
#include "aios/network/arpc/arpc/Tracer.h"
#include "aios/network/arpc/arpc/util/Log.h"
#include "aios/network/arpc/arpc/ANetRPCChannel.h"
#include "aios/autil/autil/Lock.h"

using namespace anet;

ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(SharedClientPacketHandler);

SharedClientPacketHandler::SharedClientPacketHandler() {
    _ref = 0;
    _func = NULL;
}

SharedClientPacketHandler::~SharedClientPacketHandler() { 
}

IPacketHandler::HPRetCode SharedClientPacketHandler::handlePacket(
        Packet *packet, void *args)
{
    ARPC_LOG(DEBUG, "handle package in shared client package handler");
    if (_func != NULL) {
        (*_func)();
    }
    IPacketHandler::HPRetCode ret = doHandlePacket(packet, args);
    subRef();
    return ret;
}

IPacketHandler::HPRetCode SharedClientPacketHandler::doHandlePacket(
        Packet *packet, void *args)
{
    RpcReqArg *pArgs = (RpcReqArg *)args;
    if (!packet->isRegularPacket()) {
        ARPC_LOG(WARN, "receive control packet[%d]", 
                  ((ControlPacket*)packet)->getCommand());
        return handleCmdPacket(packet, pArgs);
    }
    
    pArgs->sController->GetTracer().BeginHandleResponse();

    ARPC_LOG(TRACE1, "channel pointer: %p, msg chid: %lu", _channel,
             packet->getChannelId());
    {
        /* Check the error code and remote version. 
         * Repost the request if necessary. 
         */
        autil::ScopedLock lock(_channelLock);
        if (_channel && !_channel->CheckResponsePacket(packet, pArgs)) {
            return IPacketHandler::FREE_CHANNEL;
        }
    }

    decodePacket(pArgs->sController, packet, pArgs->sResponse,
                 pArgs->sContext->arena);
    pArgs->sClosure->Run();
    delete pArgs;

    return IPacketHandler::FREE_CHANNEL;
}

void SharedClientPacketHandler::cleanChannel() {
    autil::ScopedLock lock(_channelLock);
    setChannel(NULL);
}

void SharedClientPacketHandler::addRef() {
    autil::ScopedLock lock(_mutex);
    _ref++;
    ARPC_LOG(DEBUG, "add ref, ref count:%ld.", _ref);
}

void SharedClientPacketHandler::subRef() {
    int64_t ref = 0;
    {
        autil::ScopedLock lock(_mutex);
        ref = --_ref;
    }
    
    ARPC_LOG(DEBUG, "sub ref, ref count:%ld.", ref);
    if (ref == 0) {
        delete this;
    }
}

void SharedClientPacketHandler::setHandlePacketPreFunc(
        const std::function<void ()> *func)
{
    _func = func;
}

ARPC_END_NAMESPACE(arpc);

