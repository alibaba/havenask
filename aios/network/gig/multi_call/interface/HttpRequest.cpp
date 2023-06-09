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
#include "aios/network/gig/multi_call/interface/HttpRequest.h"
#include "autil/legacy/base64.h"
#include "aios/network/opentelemetry/core/TraceUtil.h"

using namespace std;
using namespace anet;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HttpRequest);

HttpRequest::~HttpRequest() {}

bool HttpRequest::serialize() { return serializeBody(_body); }

HTTPPacket *HttpRequest::makeHttpPacket() {
    unique_ptr<anet::HTTPPacket> packet(new anet::HTTPPacket());
    assert(_method < HM_UNKNOWN);
    if (_method == HM_GET) {
        packet->setMethod(anet::HTTPPacket::HM_GET);
    } else {
        if (!serialize()) {
            return NULL;
        }
        const auto &body = getBody();
        packet->setMethod(anet::HTTPPacket::HM_POST);
        if (!packet->setBody(body.c_str(), body.size())) {
            return NULL;
        }
    }
    packet->setURI(_uri.c_str());
    if (_keepAlive) {
        packet->addHeader("Connection", "Keep-Alive");
    } else {
        packet->addHeader("Connection", "close");
    }
    if (!_host.empty()) {
        packet->addHeader("Host", _host.c_str());
    }
    for (const auto &header : _headers) {
        packet->addHeader(header.first.c_str(), header.second.c_str());
    }
    if (!packet->getHeader(GIG_DATA.c_str())) {
        std::string gigData = getAgentQueryInfo();
        gigData = autil::legacy::Base64EncodeFast(gigData);
        packet->addHeader(GIG_DATA.c_str(), gigData.c_str());
    }
    return packet.release();
}

void HttpRequest::fillSpan() {
    auto &span = getSpan();
    if (span && span->isDebug()) {
        opentelemetry::AttributeMap attrs;
        attrs.emplace("request.host", _host);
        if (!_uri.empty() && _uri.length() < opentelemetry::kMaxRequestEventSize) {
            attrs.emplace("request.uri", _uri);
        }
        if (!_body.empty() && _body.length() < opentelemetry::kMaxRequestEventSize) {
            attrs.emplace("request.body", _body);
        }
        if (!_headers.empty()) {
            attrs.emplace("request.header", opentelemetry::TraceUtil::joinMap(_headers));
        }
        span->addEvent("request", attrs);
    }
}

} // namespace multi_call
