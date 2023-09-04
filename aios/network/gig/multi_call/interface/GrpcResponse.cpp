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
#include "aios/network/gig/multi_call/interface/GrpcResponse.h"

#include "aios/network/gig/multi_call/util/ProtobufByteBufferUtil.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GrpcResponse);

GrpcResponse::GrpcResponse() : _messageBuffer(nullptr), _message(nullptr) {
}

GrpcResponse::~GrpcResponse() {
    DELETE_AND_SET_NULL(_messageBuffer);
    freeProtoMessage(_message);
}

size_t GrpcResponse::size() const {
    if (!_messageBuffer) {
        return 0;
    } else {
        return _messageBuffer->Length();
    }
}

void GrpcResponse::init(void *data) {
    auto response = (grpc::ByteBuffer *)data;
    if (isFailed()) {
        delete response;
        return;
    }
    if (!response) {
        setErrorCode(MULTI_CALL_ERROR_RPC_FAILED);
        return;
    }
    if (_messageBuffer) {
        delete _messageBuffer;
    }
    _messageBuffer = response;
}

grpc::protobuf::Message *GrpcResponse::getMessage(google::protobuf::Message *response) {
    if (_message) {
        freeProtoMessage(_message);
    }
    _message = response;
    if (!response || !_messageBuffer) {
        return NULL;
    }
    if (ProtobufByteBufferUtil::deserializeFromBuffer(*_messageBuffer, response)) {
        return response;
    } else {
        return NULL;
    }
}

grpc::ByteBuffer *GrpcResponse::getMessageBuffer() {
    return _messageBuffer;
}

} // namespace multi_call
