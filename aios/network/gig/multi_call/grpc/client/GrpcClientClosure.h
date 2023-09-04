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
#ifndef ISEARCH_MULTI_CALL_GRPCCLIENTCLOSURE_H
#define ISEARCH_MULTI_CALL_GRPCCLIENTCLOSURE_H

#include <grpc++/grpc++.h>

#include "aios/network/gig/multi_call/service/CallBack.h"
#include "aios/network/gig/multi_call/util/ProtobufByteBufferUtil.h"

namespace multi_call {

class GrpcClientClosure
{
public:
    GrpcClientClosure(google::protobuf::Message *request, const CallBackPtr &callBack)
        : _callBack(callBack)
        , _responseBuf(new grpc::ByteBuffer())
        , _status(grpc::StatusCode::UNKNOWN, "") {
        assert(request);
        ProtobufByteBufferUtil::serializeToBuffer(*request, &_requestBuf);
    }
    virtual ~GrpcClientClosure() {
        DELETE_AND_SET_NULL(_responseBuf);
    }

private:
    GrpcClientClosure(const GrpcClientClosure &);
    GrpcClientClosure &operator=(const GrpcClientClosure &);

public:
    virtual void run(bool ok);

public:
    grpc::ByteBuffer &getRequestBuf() {
        return _requestBuf;
    }
    grpc::ByteBuffer *getResponseBuf() {
        return _responseBuf;
    }
    grpc::ByteBuffer *stealResponseBuf() {
        auto buf = _responseBuf;
        _responseBuf = NULL;
        return buf;
    }
    grpc::ClientContext *getClientContext() {
        return &_context;
    }
    grpc::Status &getStatus() {
        return _status;
    }

private:
    CallBackPtr _callBack;

    grpc::ByteBuffer _requestBuf;
    grpc::ByteBuffer *_responseBuf;
    grpc::Status _status;
    grpc::ClientContext _context;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GrpcClientClosure);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GRPCCLIENTCLOSURE_H
