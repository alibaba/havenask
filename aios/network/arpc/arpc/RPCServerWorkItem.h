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
#ifndef ARPC_RPCSERVERWORKITEM_H
#define ARPC_RPCSERVERWORKITEM_H

#include <assert.h>
#include <memory>
#include <string>

#include "aios/network/anet/anet.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/Tracer.h"
#include "autil/WorkItem.h"

ARPC_BEGIN_NAMESPACE(arpc);

class RPCServerWorkItem : public autil::WorkItem {
public:
    RPCServerWorkItem(RPCService *pService,
                      RPCMethodDescriptor *pMethodDes,
                      RPCMessage *pReqMsg,
                      const std::string &userPayload,
                      Tracer *tracer);
    virtual ~RPCServerWorkItem();

public:
    virtual void process() { assert(false); }
    virtual void destroy();
    virtual void drop();
    virtual std::string getClientAddress() = 0;
    std::string getMethodName();
    RPCMessage *getRequest();
    Tracer *getTracer() { return _tracer; }
    void setArena(const std::shared_ptr<google::protobuf::Arena> &arena) { _arena = arena; }

protected:
    RPCService *_pService;
    RPCMethodDescriptor *_pMethodDes;
    RPCMessage *_pReqMsg;
    std::string _userPayload;
    Tracer *_tracer;
    std::shared_ptr<google::protobuf::Arena> _arena;
};

ARPC_END_NAMESPACE(arpc);

#endif // ARPC_RPCSERVERWORKITEM_H
