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
#ifndef ISEARCH_MULTI_CALL_GIGRPCARG_H
#define ISEARCH_MULTI_CALL_GIGRPCARG_H

#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/arpc/arpc/MessageCodec.h"
#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {

struct GigCodecContext {
    GigCodecContext() {
        request = NULL;
        enableTrace = false;
        arena.reset(new google::protobuf::Arena());
    }

    arpc::CallId callId;
    std::string *request;
    bool enableTrace;
    std::string userPayload;
    std::shared_ptr<google::protobuf::Arena> arena;
};

struct GigRpcReqArg {
    GigRpcReqArg(arpc::ANetRPCController *controller, std::string *response,
                 RPCClosure *closure, GigCodecContext *context) {
        sController = controller;
        sResponse = response;
        sClosure = closure;
        sContext = context;
        sVersion = ARPC_VERSION_CURRENT;
    }

    ~GigRpcReqArg() { delete sContext; }
    arpc::ANetRPCController *sController;
    std::string *sResponse;
    RPCClosure *sClosure;
    GigCodecContext *sContext;
    version_t sVersion;
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGRPCARG_H
