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
#include "aios/network/gig/multi_call/grpc/server/GigGrpcClosure.h"

#include "aios/network/gig/multi_call/grpc/server/GrpcServerWorker.h"
#include "aios/network/gig/multi_call/util/ProtobufByteBufferUtil.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigGrpcClosure);

GigGrpcClosure::GigGrpcClosure(GrpcCallData *callData) : _callData(callData) {
    setNeedFinishQueryInfo(true);
}

GigGrpcClosure::~GigGrpcClosure() {
}

int64_t GigGrpcClosure::getStartTime() const {
    return _callData->beginTime;
}

void GigGrpcClosure::Run() {
    if (!_callData->response) {
        _controller.setErrorCode(MULTI_CALL_REPLY_ERROR_RESPONSE);
    }
    if (_queryInfo) {
        auto latency = (autil::TimeUtility::currentTime() - _callData->beginTime) / FACTOR_US_TO_MS;
        auto responseInfoStr =
            _queryInfo->finish(latency, _controller.getErrorCode(), getTargetWeight());
        _callData->serverContext.AddTrailingMetadata(GIG_GRPC_RESPONSE_INFO_KEY, responseInfoStr);
        fillCompatibleInfo(_callData->response, _controller.getErrorCode(), responseInfoStr);
    }
    if (_callData->response) {
        ProtobufByteBufferUtil::serializeToBuffer(*_callData->response, &_callData->sendBuffer);
    }
    writeResult();
}

ProtocolType GigGrpcClosure::getProtocolType() {
    return MC_PROTOCOL_GRPC;
}

void GigGrpcClosure::writeResult() {
    _callData->stream.WriteAndFinish(_callData->sendBuffer, grpc::WriteOptions(), grpc::Status(),
                                     _callData);
}
} // namespace multi_call
