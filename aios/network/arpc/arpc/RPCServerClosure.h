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
#ifndef ARPC_RPCSERVERCLOSURE_H
#define ARPC_RPCSERVERCLOSURE_H
#include <memory>

#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/util/Log.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/arpc/arpc/MessageCodec.h"
#include "aios/network/anet/anet.h"
#include "google/protobuf/arena.h"

ARPC_BEGIN_NAMESPACE(arpc);

class RPCServerClosure : public RPCClosure
{
public:
    RPCServerClosure(RPCMessage *reqMsg,
                     RPCMessage *resMsg);

    virtual ~RPCServerClosure();

    RPCController *GetRpcController() {
        return &_controller;
    }

    RPCMessage *GetResponseMsg() {
        return _resMsg;
    }

    RPCMessage *GetRequestMsg() {
        return _reqMsg;
    }

    void SetResponse(RPCMessage *response) {
        _resMsg = response;
    }

    void SetTracer(Tracer *tracer) {
        _tracer = tracer;
    }

    Tracer *GetTracer() {
        return _tracer;
    }

    void setProtobufArena(const std::shared_ptr<google::protobuf::Arena> &arena) {
        _arena = arena;
    }

public:
    virtual void Run();
    virtual std::string getIpAndPortAddr() = 0;
    virtual void doPostPacket() = 0;

protected:
    void release() {
        delete this;
    }
    friend class RPCServerClosureTest;
protected:
    RPCMessage *_reqMsg;
    RPCMessage *_resMsg;
    ANetRPCController _controller;
    Tracer *_tracer;
    std::shared_ptr<google::protobuf::Arena> _arena;
};

TYPEDEF_PTR(RPCServerClosure);
ARPC_END_NAMESPACE(arpc);

#endif //ARPC_RPCSERVERCLOSURE_H
