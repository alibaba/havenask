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
#ifndef ARPC_PACKETARG_H
#define ARPC_PACKETARG_H
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/stubs/port.h>
#include <stdint.h>

#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "google/protobuf/descriptor.h"
#include "aios/network/arpc/arpc/MessageCodec.h"

ARPC_BEGIN_NAMESPACE(arpc)

struct RpcReqArg {
    RpcReqArg(ANetRPCController *controller,
              RPCMessage *response,
              RPCClosure *closure,
              CodecContext *context)
    {
        sController = controller;
        sResponse = response;
        sClosure = closure;
        sContext = context;
        sVersion = ARPC_VERSION_CURRENT;
    }

    ~RpcReqArg()
    {
        delete sContext;
    }
    ANetRPCController *sController;
    RPCMessage *sResponse;
    RPCClosure *sClosure;
    CodecContext *sContext;
    version_t sVersion;
};

class PacketCodeBuilder
{
public:
    uint32_t operator () (const RPCMethodDescriptor *method)
    {
        const RPCServiceDescriptor *pSerDes = method->service();

        uint32_t serviceId = (uint32_t)(pSerDes->options().GetExtension(global_service_id));
        uint32_t methodId = (uint32_t)(method->options().GetExtension(local_method_id));

        uint32_t ret = (serviceId << 16) | methodId;

        return ret;
    }
};

class PacketCodeParser
{
public:
    void operator () (uint32_t pCode, uint16_t &serviceId, uint16_t &methodId)
    {
        serviceId = 0;
        methodId = 0;
        serviceId = pCode >> 16;
        methodId = pCode & (0xFFFF);
        return;
    }
};

ARPC_END_NAMESPACE(arpc)
#endif //ARPC_PACKETARG_H
