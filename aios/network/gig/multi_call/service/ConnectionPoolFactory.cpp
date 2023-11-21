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
#include "aios/network/gig/multi_call/service/ConnectionPoolFactory.h"

#include "aios/network/gig/multi_call/service/ArpcConnectionPool.h"
#include "aios/network/gig/multi_call/service/GrpcConnectionPool.h"
#include "aios/network/gig/multi_call/service/GrpcStreamConnectionPool.h"
#include "aios/network/gig/multi_call/service/HttpConnectionPool.h"
#include "aios/network/gig/multi_call/service/TcpConnectionPool.h"

#ifndef GLIBC_2_17_COMPATIBLE
#endif

using namespace std;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, ConnectionPoolFactory);

ConnectionPoolPtr ConnectionPoolFactory::createConnectionPool(ProtocolType type,
                                                              const std::string &typeStr) {
    switch (type) {
    case MC_PROTOCOL_TCP:
        return ConnectionPoolPtr(new TcpConnectionPool());
    case MC_PROTOCOL_ARPC:
        if (typeStr == MC_PROTOCOL_RAW_ARPC_STR) {
            return ConnectionPoolPtr(new ArpcConnectionPool(true));
        } else {
            return ConnectionPoolPtr(new ArpcConnectionPool(false));
        }
    case MC_PROTOCOL_HTTP:
        return ConnectionPoolPtr(new HttpConnectionPool());
    case MC_PROTOCOL_GRPC:
        return ConnectionPoolPtr(new GrpcConnectionPool());
    case MC_PROTOCOL_GRPC_STREAM:
        return ConnectionPoolPtr(new GrpcStreamConnectionPool());
#ifndef GLIBC_2_17_COMPATIBLE
#endif
    default:
        AUTIL_LOG(ERROR, "create ConnectionPool failed, unknown type [%u]", type);
        return ConnectionPoolPtr();
    }
}

} // namespace multi_call
