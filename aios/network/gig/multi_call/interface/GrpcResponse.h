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
#ifndef ISEARCH_MULTI_CALL_GRPCRESPONSE_H
#define ISEARCH_MULTI_CALL_GRPCRESPONSE_H

#include "aios/network/gig/multi_call/interface/Response.h"
#include <grpc++/impl/codegen/byte_buffer.h>
#include <grpc++/impl/codegen/proto_utils.h>

namespace multi_call {

class GrpcResponse : public Response {
public:
    GrpcResponse();
    ~GrpcResponse();

private:
    GrpcResponse(const GrpcResponse &);
    GrpcResponse &operator=(const GrpcResponse &);

public:
    void init(void *data) override;
    size_t size() const override;
    grpc::protobuf::Message *getMessage(grpc::protobuf::Message *response);
    grpc::ByteBuffer *getMessageBuffer();

private:
    grpc::ByteBuffer *_messageBuffer;
    google::protobuf::Message *_message;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GrpcResponse);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GRPCRESPONSE_H
