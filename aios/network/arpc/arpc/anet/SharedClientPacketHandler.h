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
#ifndef ARPC_SHAREDCLIENTPACKETHANDLER_H
#define ARPC_SHAREDCLIENTPACKETHANDLER_H
#include <stdint.h>
#include <functional>

#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/anet/ClientPacketHandler.h"
#include "aios/network/arpc/arpc/util/Log.h"
#include "aios/network/anet/ipackethandler.h"
#include "aios/autil/autil/Lock.h"

ARPC_BEGIN_NAMESPACE(arpc);

class SharedClientPacketHandler : public ClientPacketHandler
{
public:
    SharedClientPacketHandler();
protected:
    virtual ~SharedClientPacketHandler();
    
public:
    anet::IPacketHandler::HPRetCode handlePacket(anet::Packet *packet, void *args);
    void addRef();
    void subRef();
    void cleanChannel();

private:
    anet::IPacketHandler::HPRetCode doHandlePacket(
            anet::Packet *packet, void *args);
private:
    //for test
    void setHandlePacketPreFunc(const std::function<void ()> *func);
    friend class ANetRPCChannelCloseBugTest;
    
private:
    int64_t _ref;
    mutable autil::ThreadMutex _mutex;
    mutable autil::ThreadMutex _channelLock;
    const std::function<void ()> *_func;
};

TYPEDEF_PTR(SharedClientPacketHandler);
ARPC_END_NAMESPACE(arpc);

#endif //ARPC_SHAREDCLIENTPACKETHANDLER_H
