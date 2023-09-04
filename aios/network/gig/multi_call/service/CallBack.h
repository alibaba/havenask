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
#ifndef ISEARCH_MULTI_CALL_CALLBACK_H
#define ISEARCH_MULTI_CALL_CALLBACK_H

#include <functional>

#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Response.h"

namespace multi_call {

class Caller;
typedef std::shared_ptr<Caller> CallerPtr;
class SearchServiceResource;

class CallBack
{
public:
    CallBack(const std::shared_ptr<SearchServiceResource> &resource, const CallerPtr &caller,
             bool isRetry);
    virtual ~CallBack();

private:
    CallBack(const CallBack &);
    CallBack &operator=(const CallBack &);

public:
    // virtual for ut
    virtual void run(void *responseData, MultiCallErrorCode ec, const std::string &errorString,
                     const std::string &responseInfo);
    RequestType getRequestType() const;
    bool isCopyRequest() const;
    bool isRetry() const {
        return _isRetry;
    }
    const std::shared_ptr<SearchServiceResource> &getResource() const {
        return _resource;
    }
    const opentelemetry::SpanPtr &getSpan() {
        return _span;
    }
    void setSpan(const opentelemetry::SpanPtr &span) {
        _span = span;
    }
    void setProtobufArena(const std::shared_ptr<google::protobuf::Arena> &arena) {
        _arena = arena;
    }
    void rpcBegin();
    void rpcEnd();

private:
    std::shared_ptr<google::protobuf::Arena> _arena;
    std::shared_ptr<SearchServiceResource> _resource;
    CallerPtr _caller;
    bool _isRetry;
    bool _needRpcBegin;
    opentelemetry::SpanPtr _span;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(CallBack);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_CALLBACK_H
