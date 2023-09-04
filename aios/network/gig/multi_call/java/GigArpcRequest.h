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
#ifndef ISEARCH_MULTI_CALL_GIGARPCREQUEST_H
#define ISEARCH_MULTI_CALL_GIGARPCREQUEST_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Request.h"
#include "aios/network/gig/multi_call/java/GigArpcResponse.h"
#include "autil/StringUtil.h"
#include "autil/legacy/base64.h"

namespace multi_call {

class GigArpcRequest : public Request
{
public:
    GigArpcRequest(const std::shared_ptr<google::protobuf::Arena> &arena =
                       std::shared_ptr<google::protobuf::Arena>())
        : Request(MC_PROTOCOL_ARPC, arena)
        , _serviceId(0)
        , _methodId(0)
        , _packet("") {
    }
    ~GigArpcRequest() {
    }

private:
    GigArpcRequest(const GigArpcRequest &);
    GigArpcRequest &operator=(const GigArpcRequest &);

public:
    ResponsePtr newResponse() override {
        return ResponsePtr(new GigArpcResponse());
    }

    bool serialize() override {
        return true;
    }

    size_t size() const override {
        return _packet.length();
    }

public:
    void setServiceId(uint32_t sId) {
        _serviceId = sId;
    }
    uint32_t getServiceId() {
        return _serviceId;
    }
    void setMethodId(uint32_t mId) {
        _methodId = mId;
    }
    uint32_t getMethodId() {
        return _methodId;
    }
    void setPacket(const std::string &packet) {
        _packet = packet;
    }
    const std::string &getPacket() const {
        return _packet;
    }
    void fillSpan() override {
        auto &span = getSpan();
        if (span && span->isDebug()) {
            opentelemetry::AttributeMap attrs;
            attrs.emplace("request.service_id", autil::StringUtil::toString(_serviceId));
            attrs.emplace("request.method_id", autil::StringUtil::toString(_methodId));
            std::string messageStr = autil::legacy::Base64EncodeFast(_packet);
            if (!messageStr.empty() && messageStr.length() < opentelemetry::kMaxRequestEventSize) {
                attrs.emplace("request.message", messageStr);
            }
            span->addEvent("request", attrs);
        }
    }

private:
    uint32_t _serviceId;
    uint32_t _methodId;
    std::string _packet;
};

MULTI_CALL_TYPEDEF_PTR(GigArpcRequest);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGARPCREQUEST_H
