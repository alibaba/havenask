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
#ifndef ARPC_COMMON_MACROS_H
#define ARPC_COMMON_MACROS_H

#include <functional>
#include <google/protobuf/message_lite.h>
#include <google/protobuf/stubs/callback.h>
#include <google/protobuf/stubs/port.h>
#include <memory>
#include <stdint.h>
#include <stdio.h>

#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "google/protobuf/service.h"
#include "google/protobuf/stubs/common.h"

#ifndef ARPC_VERSION
#define ARPC_VERSION 001500
#endif
#define ARPC_BEGIN_NAMESPACE(x) namespace x {
#define ARPC_END_NAMESPACE(x) }
#define ARPC_BEGIN_SUB_NAMESPACE(x)                                                                                    \
    namespace arpc {                                                                                                   \
    namespace x {
#define ARPC_END_SUB_NAMESPACE(x)                                                                                      \
    }                                                                                                                  \
    }

#define ARPC_USE_NAMESPACE(x) using namespace arpc::x

#define TYPEDEF_GOOGLE_TYPE(g, a) typedef google::protobuf::g a

TYPEDEF_GOOGLE_TYPE(Service, RPCService);
TYPEDEF_GOOGLE_TYPE(RpcChannel, RPCChannel);
TYPEDEF_GOOGLE_TYPE(Message, RPCMessage);
TYPEDEF_GOOGLE_TYPE(RpcController, RPCController);
TYPEDEF_GOOGLE_TYPE(Closure, RPCClosure);
TYPEDEF_GOOGLE_TYPE(MethodDescriptor, RPCMethodDescriptor);
TYPEDEF_GOOGLE_TYPE(ServiceDescriptor, RPCServiceDescriptor);
TYPEDEF_GOOGLE_TYPE(int64, RPCint64);
TYPEDEF_GOOGLE_TYPE(io::ZeroCopyOutputStream, RPCZeroCopyOutputStream);

#define TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;

typedef uint8_t version_t;
typedef uint64_t channelid_t;
typedef std::function<void()> ARPCMethodType;

#define ARPC_VERSION_0 0
#define ARPC_VERSION_1 1
#define ARPC_VERSION_CURRENT ARPC_VERSION_1

#define DEFAULT_PAKCET_TYPE 0

#define DEFAULT_THREADPOOL_NAME "ArpcTpDefault"
#define IO_THREADPOOL_NAME "_arpc_threadpool_io_"
#define DEFAULT_TREAHDPOOL_NAME DEFAULT_THREADPOOL_NAME
#define ARPC_CONN_IDEL_TIME 900000  // 15min
#define ARPC_BUFFER_BLOCK_SIZE 1024 // 1k

// the same limit for protobuf 2.3.0 & 2.4.1
// refer to: google/protobuf/io/coded_stream.h
#define MAX_RPC_MSG_BYTE_SIZE (1 << 30)

// the max size of host name
#define HOST_NAME_MAX 64

namespace arpc {
#define errorCodeToString(code) ErrorCode_Name((arpc::ErrorCode)(code))
} // namespace arpc

inline void freeProtoMessage(const google::protobuf::Message *message) {
    if (message && !message->GetArena()) {
        delete message;
    }
}

inline google::protobuf::Arena *getArena(const google::protobuf::Message *message,
                                         const std::shared_ptr<google::protobuf::Arena> &arenaPtr) {
    auto arena = message->GetArena();
    if (!arena) {
        arena = arenaPtr.get();
    }
    return arena;
}

#endif // end of ARPC_COMMON_MACROS_H
