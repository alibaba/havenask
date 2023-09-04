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
#include "aios/network/gig/multi_call/interface/TcpRequest.h"

#include "autil/legacy/base64.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, TcpRequest);

TcpRequest::~TcpRequest() {
}

bool TcpRequest::serialize() {
    return serializeBody(_body);
}

anet::DefaultPacket *TcpRequest::makeTcpPacket() {
    unique_ptr<anet::DefaultPacket> packet(new anet::DefaultPacket());
    if (!serialize()) {
        return NULL;
    }
    std::string body = getBody();
    // append gig meta to tcp body
    if (_metaByTcp != META_BT_NONE && body.find(GIG_DATA_C) == std::string::npos) {
        std::string gigData = autil::legacy::Base64EncodeFast(getAgentQueryInfo());

        switch (_metaByTcp) {
        case META_BT_IGRAPH: {
            std::size_t pos = body.find("?config%7B");
            if (pos != std::string::npos) {
                std::string meta;
                if (pos > 0) {
                    meta.append("&");
                }
                meta.append(GIG_DATA_C).append("=").append(gigData);
                body.insert(pos, meta);
                break;
            }
            // fall through
        }
        case META_BT_APPEND:
            if (body.length() > 0) {
                body.append("&");
            }
            body.append(GIG_DATA_C).append("=").append(gigData);
            break;
        default:
            break;
        }
    }

    if (!packet->setBody(body.c_str(), body.length())) {
        return NULL;
    }
    return packet.release();
}

void TcpRequest::fillSpan() {
    auto &span = getSpan();
    if (span && span->isDebug()) {
        opentelemetry::AttributeMap attrs;
        if (!_body.empty() && _body.length() < opentelemetry::kMaxRequestEventSize) {
            attrs.emplace("request.body", _body);
        }
        span->addEvent("request", attrs);
    }
}

} // namespace multi_call
