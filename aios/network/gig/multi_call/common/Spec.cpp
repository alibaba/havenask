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
#include "aios/network/gig/multi_call/common/Spec.h"

#include "autil/StringConvertor.h"

using namespace std;
using namespace autil;

namespace multi_call {

Spec::Spec() {
    ports[MC_PROTOCOL_TCP] = INVALID_PORT;
    ports[MC_PROTOCOL_ARPC] = INVALID_PORT;
    ports[MC_PROTOCOL_HTTP] = INVALID_PORT;
    ports[MC_PROTOCOL_GRPC] = INVALID_PORT;
    ports[MC_PROTOCOL_GRPC_STREAM] = INVALID_PORT;
    ports[MC_PROTOCOL_RDMA_ARPC] = INVALID_PORT;
    ports[MC_PROTOCOL_UNKNOWN] = INVALID_PORT;
}

bool Spec::operator==(const Spec &rhs) const {
    return ip == rhs.ip && ports[MC_PROTOCOL_TCP] == rhs.ports[MC_PROTOCOL_TCP] &&
           ports[MC_PROTOCOL_ARPC] == rhs.ports[MC_PROTOCOL_ARPC] &&
           ports[MC_PROTOCOL_HTTP] == rhs.ports[MC_PROTOCOL_HTTP] &&
           ports[MC_PROTOCOL_GRPC] == rhs.ports[MC_PROTOCOL_GRPC] &&
           ports[MC_PROTOCOL_GRPC_STREAM] == rhs.ports[MC_PROTOCOL_GRPC_STREAM] &&
           ports[MC_PROTOCOL_RDMA_ARPC] == rhs.ports[MC_PROTOCOL_RDMA_ARPC];
}

bool Spec::operator!=(const Spec &rhs) const {
    return !(*this == rhs);
}

std::string Spec::getAnetSpec(ProtocolType type) const {
    auto spec = getSpecStr(type);
    if (type != MC_PROTOCOL_GRPC && type != MC_PROTOCOL_GRPC_STREAM) {
        return "tcp:" + spec;
    } else {
        return spec;
    }
}

std::string Spec::getSpecStr(ProtocolType type) const {
    size_t maxLength = ip.length() + 65;
    StringAppender appender(maxLength);
    appender.appendString(ip).appendChar(':').appendInt64(ports[type]);
    return appender.toString();
}

bool Spec::hasValidPort() const {
    for (uint32_t type = 0; type < MC_PROTOCOL_UNKNOWN; ++type) {
        if (INVALID_PORT != ports[type]) {
            return true;
        }
    }
    return false;
}

std::ostream &operator<<(std::ostream &os, const Spec &spec) {
    os << spec.ip << ":";
    for (uint32_t type = 0; type < MC_PROTOCOL_UNKNOWN; ++type) {
        os << (type > 0 ? ":" : "") << spec.ports[type];
    }
    return os;
}

} // namespace multi_call
