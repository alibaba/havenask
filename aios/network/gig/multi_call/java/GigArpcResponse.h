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
#ifndef ISEARCH_MULTI_CALL_GIGARPCRESPONSE_H
#define ISEARCH_MULTI_CALL_GIGARPCRESPONSE_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Response.h"
#include "autil/legacy/base64.h"

namespace multi_call {

class GigArpcResponse : public Response
{
public:
    GigArpcResponse() {
    }
    ~GigArpcResponse() {
    }

private:
    GigArpcResponse(const GigArpcResponse &);
    GigArpcResponse &operator=(const GigArpcResponse &);

public:
    void init(void *data) override {
        // save response data
        std::string *dataStr = static_cast<std::string *>(data);
        if (!dataStr) {
            return;
        }
        _data = *dataStr;
    }
    bool deserializeApp() override {
        return true;
    }
    size_t size() const override {
        return _data.size();
    }
    std::string &getData() {
        return _data;
    }
    void fillSpan() override {
        auto &span = getSpan();
        if (span && span->isDebug()) {
            opentelemetry::AttributeMap attrs;
            std::string messageStr = autil::legacy::Base64EncodeFast(_data);
            if (!messageStr.empty() && messageStr.length() < opentelemetry::kMaxResponseEventSize) {
                attrs.emplace("response.message", messageStr);
            }
            span->addEvent("response", attrs);
        }
    }

private:
    std::string _data;
};

MULTI_CALL_TYPEDEF_PTR(GigArpcResponse);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGARPCRESPONSE_H
