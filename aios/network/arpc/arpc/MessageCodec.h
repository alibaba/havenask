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
#ifndef ARPC_MESSAGECODEC_H
#define ARPC_MESSAGECODEC_H
#include <assert.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "aios/network/anet/anet.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/Tracer.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/arpc/arpc/util/Log.h"

ARPC_BEGIN_NAMESPACE(arpc);

struct CallId {
    std::string strId;
    uint32_t intId;
};

struct CodecContext {
    CodecContext() {
        request = NULL;
        rpcService = NULL;
        rpcMethodDes = NULL;
        errorCode = ARPC_ERROR_NONE;
        enableTrace = false;
        ownRequest = false;
        timeout = 0;
        qosId = 0;
        requestId = 0;
    }

    CallId callId;          // for encode
    RPCMessage *request;    // for en/decode
    std::string token;      // for en/decode
    RPCService *rpcService; // for decode
    std::shared_ptr<RPCService> rpcServicePtr;
    RPCMethodDescriptor *rpcMethodDes; // for decode
    ErrorCode errorCode;
    bool enableTrace;
    bool ownRequest;
    int32_t timeout;
    std::string clientName;
    uint32_t qosId;
    std::string userPayload;
    std::shared_ptr<google::protobuf::Arena> arena;
    uint32_t requestId;
};

class RPCServer;

class MessageCodec {
public:
    MessageCodec();
    virtual ~MessageCodec();

public:
    virtual CallId GenerateCallId(const RPCMethodDescriptor *method, version_t version) const;

protected:
    friend class MessageCodecTest;
};

TYPEDEF_PTR(MessageCodec);
ARPC_END_NAMESPACE(arpc);

#endif // ARPC_MESSAGECODEC_H
