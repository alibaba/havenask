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
#include "aios/network/gig/multi_call/rpc/GigClosure.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/gig/multi_call/agent/QueryInfo.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/util/ProtobufUtil.h"
#include "aios/network/http_arpc/HTTPRPCController.h"
#include "autil/Log.h"
#include <grpc++/impl/codegen/proto_utils.h>

using namespace std;
using namespace google::protobuf;

namespace multi_call {

AUTIL_DECLARE_AND_SETUP_LOGGER(multi_call, GigClosure);

GigClosure::GigClosure()
    : _compatibleInfo(nullptr), _needFinishQueryInfo(false),
      _targetWeight(MAX_WEIGHT), _startTime(autil::TimeUtility::currentTime()) {
}

GigClosure::~GigClosure() {
    if (_needFinishQueryInfo && _queryInfo && !_queryInfo->isFinished()) {
        AUTIL_INTERVAL_LOG(600, ERROR,
                           "query info not finished[%s], "
                           "this may lead to serious gig misbehavior",
                           _queryInfo->toString().c_str());
    }
    endTrace();
}

const QuerySessionPtr &GigClosure::getQuerySession() const {
    return _querySession;
}

void GigClosure::setQuerySession(const QuerySessionPtr &session) {
    _querySession = session;
}

void GigClosure::setQueryInfo(const QueryInfoPtr &queryInfo) {
    _queryInfo = queryInfo;
}

const QueryInfoPtr &GigClosure::getQueryInfo() const { return _queryInfo; }

void GigClosure::setNeedFinishQueryInfo(bool needFinish) {
    _needFinishQueryInfo = needFinish;
}

int64_t GigClosure::getStartTime() const { return _startTime; }

GigRpcController &GigClosure::getController() { return _controller; }

MultiCallErrorCode GigClosure::getErrorCode() const {
    return _controller.getErrorCode();
}

void GigClosure::setErrorCode(MultiCallErrorCode ec) {
    _controller.setErrorCode(ec);
}

void GigClosure::setTargetWeight(WeightTy targetWeight) {
    _targetWeight = targetWeight;
}

WeightTy GigClosure::getTargetWeight() const { return _targetWeight; }

RequestType GigClosure::getRequestType() const {
    if (_queryInfo) {
        return _queryInfo->requestType();
    } else {
        return RT_NORMAL;
    }
}

float GigClosure::degradeLevel(float level) const {
    if (!_queryInfo) {
        return MIN_PERCENT;
    }
    return _queryInfo->degradeLevel(level);
}

void GigClosure::setCompatibleFieldInfo(const CompatibleFieldInfo *info) {
    _compatibleInfo = info;
}

void GigClosure::fillCompatibleInfo(google::protobuf::Message *message,
                                    MultiCallErrorCode ec,
                                    const std::string &responseInfo) const {
    if (!_compatibleInfo) {
        return;
    }
    ProtobufCompatibleUtil::setErrorCodeField(message, _compatibleInfo->ecField,
                                              ec);
    ProtobufCompatibleUtil::setGigMetaField(
        message, _compatibleInfo->responseInfoField, responseInfo);
}

void GigClosure::startTrace() {
    if (!_querySession) {
        return;
    }
    auto span = _querySession->getTraceServerSpan();
    if (!span) {
        return;
    }
    auto rpcController = _controller.getRpcController();
    if (rpcController) {
        std::string remoteIp;
        if (auto arpcController =
                dynamic_cast<arpc::ANetRPCController *>(rpcController)) {
            remoteIp = arpcController->GetClientAddress();
        } else if (auto httpController =
                       dynamic_cast<http_arpc::HTTPRPCControllerBase *>(
                           rpcController)) {
            remoteIp = httpController->GetAddr();
        }
        span->setAttribute(opentelemetry::kSpanAttrNetPeerIp, remoteIp);
    }
}

void GigClosure::endTrace() {
    if (!_querySession) {
        return;
    }
    auto span = _querySession->getTraceServerSpan();
    if (!span) {
        return;
    }
}

} // namespace multi_call
