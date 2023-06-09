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
#include <memory>

#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/util/Log.h"
#include "aios/network/arpc/arpc/RPCServerClosure.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/arpc/arpc/anet/ANetRPCMessageCodec.h"
#include "aios/network/anet/anet.h"
#include "google/protobuf/arena.h"

ARPC_BEGIN_NAMESPACE(arpc);

class ANetRPCServerClosure : public RPCServerClosure
{
public:
    ANetRPCServerClosure(channelid_t chid,
                         anet::Connection *connection,
                         RPCMessage *reqMsg,
                         RPCMessage *resMsg,
                         ANetRPCMessageCodec *messageCodec,
                         version_t version);

    virtual ~ANetRPCServerClosure();

    std::string getIpAndPortAddr() override;
public:
    // for test
    anet::Connection *GetConnection() const
    {
        return _connection;
    }

    channelid_t GetChid() const
    {
        return _chid;
    }
    anet::Packet *buildResponsePacket();

protected:
    virtual void doPostPacket() override;
    friend class RPCServerClosureTest;
private:
    //need by anet
    channelid_t _chid;
    anet::Connection *_connection;
    ANetRPCMessageCodec *_messageCodec;
    version_t _version;
};

TYPEDEF_PTR(RPCServerClosure);

ARPC_END_NAMESPACE(arpc);