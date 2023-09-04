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
#ifndef ISEARCH_MULTI_CALL_GIGJAVACLOSURE_H
#define ISEARCH_MULTI_CALL_GIGJAVACLOSURE_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Closure.h"
#include "aios/network/gig/multi_call/interface/Reply.h"
#include "aios/network/gig/multi_call/java/GigApi.h"
#include "aios/network/gig/multi_call/java/GigRequestGenerator.h"
#include "aios/network/gig/multi_call/proto/GigCallProto.pb.h"

namespace multi_call {

class GigJavaClosure : public Closure
{
public:
    GigJavaClosure(JavaCallback callback, long callbackId, const GigRequestGeneratorPtr &generator)
        : _callback(callback)
        , _callbackId(callbackId)
        , _arena(generator->getProtobufArena())
        , _isMulti(generator->isMulti())
        , _requestId(generator->getRequestId()) {
    }
    virtual ~GigJavaClosure() {
        _arena.reset();
    }

private:
    GigJavaClosure(const GigJavaClosure &);
    GigJavaClosure &operator=(const GigJavaClosure &);

public:
    virtual void Run() override;
    virtual bool extractResponse(ResponsePtr response, GigResponseHeader *responseHeader,
                                 const char *&body, size_t &bodySize) = 0;
    bool extract(ResponsePtr response, GigResponseHeader *responseHeader, const char *&body,
                 size_t &bodySize);

public:
    ReplyPtr &getReply() {
        return _reply;
    }
    bool isMulti() const {
        return _isMulti;
    }
    void setSpan(const opentelemetry::SpanPtr &span) {
        _span = span;
    }

protected:
    JavaCallback _callback;
    long _callbackId;
    std::shared_ptr<google::protobuf::Arena> _arena;
    ReplyPtr _reply;
    bool _isMulti;
    std::string _requestId;
    opentelemetry::SpanPtr _span;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGJAVACLOSURE_H
