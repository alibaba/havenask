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
#include "aios/network/gig/multi_call/service/ArpcConnectionBase.h"

#include <typeinfo>

#include "aios/network/arpc/arpc/RPCChannelBase.h"
#include "aios/network/gig/multi_call/interface/ArpcRequest.h"
#include "aios/network/gig/multi_call/java/GigArpcRequest.h"
#include "aios/network/gig/multi_call/java/arpc/GigArpcCallBack.h"
#include "aios/network/gig/multi_call/java/arpc/GigRPCChannel.h"
#include "aios/network/gig/multi_call/service/ProtocolCallBack.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"
#include "aios/network/gig/multi_call/util/ProtobufUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ArpcConnectionBase);

ArpcConnectionBase::ArpcConnectionBase(const std::string &spec, ProtocolType type, size_t queueSize)
    : Connection(spec, type, queueSize) {
}

ArpcConnectionBase::~ArpcConnectionBase() {
}

void ArpcConnectionBase::postById(const RequestPtr &request, const CallBackPtr &callBack) {
    auto gigArpcRequest = dynamic_cast<GigArpcRequest *>(request.get());
    if (!gigArpcRequest) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_REQUEST, string(), string());
        return;
    }
    auto channel = getChannel();
    GigRPCChannelPtr gigChannel = dynamic_pointer_cast<GigRPCChannel>(channel);
    if (!gigChannel) {
        AUTIL_LOG(ERROR, "dynamic cast to GigRPCChannel failed!");
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_CONNECTION, string(), string());
        return;
    }

    const auto &resource = callBack->getResource();
    gigArpcRequest->setRequestType(resource->getRequestType());
    if (!gigArpcRequest->serialize()) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_PROTOCOL_MESSAGE, string(), string());
        return;
    }
    auto message = gigArpcRequest->getPacket();
    if (message.empty()) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_PROTOCOL_MESSAGE, string(), string());
        return;
    }

    if (resource->isNormalRequest()) {
        startChildRpc(request, callBack);
    }

    auto closure = new GigArpcCallBack(message, callBack, _callBackThreadPool);
    auto controller = closure->getController();
    controller->SetExpireTime(resource->getTimeout());
    auto queryInfo = gigArpcRequest->getAgentQueryInfo();
    auto &tracer = controller->GetTracer();
    tracer.setUserPayload(queryInfo);
    tracer.SetTraceFlag(true);

    uint32_t serviceId = gigArpcRequest->getServiceId();
    uint32_t methodId = gigArpcRequest->getMethodId();
    const std::string &requestStr = closure->getRequestStr();
    std::string &responseStr = closure->getResponseStr();

    callBack->rpcBegin();
    gigChannel->CallMethod(serviceId, methodId, controller, &requestStr, &responseStr, closure);
}

void ArpcConnectionBase::postByStub(const RequestPtr &request, const CallBackPtr &callBack) {
    assert(callBack);
    const auto &resource = callBack->getResource();
    auto arpcRequest = dynamic_cast<ArpcRequestBase *>(request.get());
    if (!arpcRequest) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_REQUEST, string(), string());
        return;
    }
    auto channel = getChannel();
    if (!channel) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_CONNECTION, string(), string());
        return;
    }
    auto stub = arpcRequest->createStub(channel.get());
    if (!stub) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_REQUEST, string(), string());
        return;
    }

    arpcRequest->setRequestType(resource->getRequestType());
    if (!arpcRequest->serialize()) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_PROTOCOL_MESSAGE, string(), string());
        return;
    }
    auto message = arpcRequest->getCopyMessage();
    if (!message) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_PROTOCOL_MESSAGE, string(), string());
        return;
    }
    const auto &methodName = arpcRequest->getMethodName();
    auto methodDes = stub->GetDescriptor()->FindMethodByName(methodName);
    if (!methodDes) {
        AUTIL_LOG(WARN, "can not find method[%s].", methodName.c_str());
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_REQUEST, string(), string());
        return;
    }

    if (resource->isNormalRequest()) {
        startChildRpc(request, callBack);
    }

    auto arena = request->getProtobufArena().get();
    google::protobuf::Message *response = NULL;
    if (likely((bool)arena)) {
        response = stub->GetResponsePrototype(methodDes).New(arena);
    } else {
        response = stub->GetResponsePrototype(methodDes).New();
    }
    assert(response);
    auto closure = new ArpcCallBack(message, response, callBack, _callBackThreadPool);
    auto controller = closure->getController();
    controller->SetExpireTime(resource->getTimeout());
    auto queryInfo = arpcRequest->getAgentQueryInfo();
    auto &tracer = controller->GetTracer();
    tracer.setUserPayload(queryInfo);
    tracer.SetTraceFlag(true);
    const auto &flowControlStrategy = resource->getFlowControlConfig();
    if (flowControlStrategy) {
        const auto &requestInfoField = flowControlStrategy->compatibleFieldInfo.requestInfoField;
        ProtobufCompatibleUtil::setGigMetaField(message, requestInfoField, queryInfo);
    }
    callBack->rpcBegin();
    stub->CallMethod(methodDes, controller, message, response, closure);
}

} // namespace multi_call