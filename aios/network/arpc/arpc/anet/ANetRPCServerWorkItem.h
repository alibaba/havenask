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

#include "aios/network/anet/anet.h"
#include "aios/network/arpc/arpc/RPCServerWorkItem.h"
#include "aios/network/arpc/arpc/anet/ANetRPCMessageCodec.h"

ARPC_BEGIN_NAMESPACE(arpc)

class ANetRPCServerWorkItem : public RPCServerWorkItem
{
public:
    ANetRPCServerWorkItem(RPCService *pService,
                          RPCMethodDescriptor *pMethodDes,
                          RPCMessage *pReqMsg,
                          const std::string &userPayload,
                          anet::Connection *pConnection,
                          channelid_t channelId,
                          ANetRPCMessageCodec *messageCodec,
                          version_t version,
                          Tracer *tracer);

    virtual ~ANetRPCServerWorkItem();

public:
    virtual void destroy() override;
    virtual void process() override;
    virtual std::string getClientAddress() override;

private:
    anet::Connection *_pConnection;
    channelid_t _channelId;
    version_t _version;
    ANetRPCMessageCodec *_messageCodec;
};

ARPC_END_NAMESPACE(arpc)