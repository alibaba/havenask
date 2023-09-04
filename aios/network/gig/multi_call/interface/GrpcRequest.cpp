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
#include "aios/network/gig/multi_call/interface/GrpcRequest.h"

#include "aios/network/gig/multi_call/interface/GrpcResponse.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GrpcRequest);

GrpcRequest::GrpcRequest(const std::string &methodName,
                         const std::shared_ptr<google::protobuf::Arena> &arena)
    : Request(MC_PROTOCOL_GRPC, arena)
    , _methodName(methodName)
    , _message(NULL) {
}

GrpcRequest::~GrpcRequest() {
    DELETE_AND_SET_NULL(_message);
}

ResponsePtr GrpcRequest::newResponse() {
    return ResponsePtr(new GrpcResponse());
}

const std::string &GrpcRequest::getMethodName() const {
    return _methodName;
}
void GrpcRequest::setMessage(google::protobuf::Message *msg) {
    _message = msg;
}

google::protobuf::Message *GrpcRequest::getMessage() const {
    return _message;
}

} // namespace multi_call
