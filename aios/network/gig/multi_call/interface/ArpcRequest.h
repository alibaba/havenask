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
#ifndef ISEARCH_MULTI_CALL_ARPCREQUEST_H
#define ISEARCH_MULTI_CALL_ARPCREQUEST_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Request.h"
#include "google/protobuf/message.h"
#include "google/protobuf/service.h"

namespace multi_call {

class ArpcRequestBase : public Request
{
public:
    ArpcRequestBase(const std::string &methodName,
                    const std::shared_ptr<google::protobuf::Arena> &arena)
        : Request(MC_PROTOCOL_ARPC, arena)
        , _message(NULL) {
        setMethodName(methodName);
    }
    ~ArpcRequestBase() {
        freeProtoMessage(_message);
        _message = NULL;
    }

public:
    virtual google::protobuf::Service *createStub(google::protobuf::RpcChannel *channel) = 0;
    virtual google::protobuf::Message *serializeMessage() {
        return _message;
    }

public:
    bool serialize() override {
        setMessage(serializeMessage());
        return _message != nullptr;
    }
    size_t size() const override {
        if (_message) {
            return _message->ByteSize();
        } else {
            return 0;
        }
    }
    google::protobuf::Message *getMessage() const {
        return _message;
    }
    google::protobuf::Message *getCopyMessage() const {
        if (_message) {
            auto msg = _message->New(getProtobufArena().get());
            msg->CopyFrom(*_message);
            return msg;
        }
        return NULL;
    }
    void setMessage(google::protobuf::Message *message) {
        if (_message != message) {
            freeProtoMessage(_message);
            _message = message;
        }
    }
    void fillSpan() override {
        auto &span = getSpan();
        if (span && span->isDebug() && _message) {
            opentelemetry::AttributeMap attrs;
            std::string messageStr = _message->ShortDebugString();
            if (!messageStr.empty() && messageStr.length() < opentelemetry::kMaxRequestEventSize) {
                attrs.emplace("request.message", messageStr);
            }
            span->addEvent("request", attrs);
        }
    }
    void enableRdma() {
        _type = MC_PROTOCOL_RDMA_ARPC;
    }

protected:
    google::protobuf::Message *_message;
};

MULTI_CALL_TYPEDEF_PTR(ArpcRequestBase);

template <typename StubType>
class ArpcRequest : public ArpcRequestBase
{
public:
    ArpcRequest(const std::string &methodName,
                const std::shared_ptr<google::protobuf::Arena> &arena)
        : ArpcRequestBase(methodName, arena)
        , _stub(NULL) {
    }
    ~ArpcRequest() {
        DELETE_AND_SET_NULL(_stub);
    }

private:
    ArpcRequest(const ArpcRequest &);
    ArpcRequest &operator=(const ArpcRequest &);

public:
    google::protobuf::Service *createStub(google::protobuf::RpcChannel *channel) override {
        if (_stub) {
            // retry
            delete _stub;
        }
        _stub = new StubType(channel, google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
        return _stub;
    }

private:
    StubType *_stub;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_ARPCREQUEST_H
