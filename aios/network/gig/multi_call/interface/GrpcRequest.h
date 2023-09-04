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
#ifndef ISEARCH_MULTI_CALL_GRPCREQUEST_H
#define ISEARCH_MULTI_CALL_GRPCREQUEST_H

#include <grpc++/impl/codegen/proto_utils.h>

#include "aios/network/gig/multi_call/interface/Request.h"

namespace multi_call {

class GrpcRequest : public Request
{
public:
    GrpcRequest(const std::string &methodName,
                const std::shared_ptr<google::protobuf::Arena> &arena);
    ~GrpcRequest();

private:
    GrpcRequest(const GrpcRequest &);
    GrpcRequest &operator=(const GrpcRequest &);

public:
    ResponsePtr newResponse() override;
    bool serialize() override {
        setMessage(serializeMessage());
        return true;
    }
    size_t size() const override {
        if (_message) {
            return _message->ByteSize();
        } else {
            return 0;
        }
    }

public:
    virtual google::protobuf::Message *serializeMessage() {
        return _message;
    }

public:
    const std::string &getMethodName() const;
    void setMessage(grpc::protobuf::Message *msg);
    grpc::protobuf::Message *getMessage() const;

private:
    std::string _methodName;
    grpc::protobuf::Message *_message;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GrpcRequest);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GRPCREQUEST_H
