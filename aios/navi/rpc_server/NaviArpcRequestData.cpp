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
#include "navi/rpc_server/NaviArpcRequestData.h"
#include "aios/network/gig/multi_call/rpc/GigClosure.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/http_arpc/HTTPRPCControllerBase.h"

namespace navi {

const std::string NaviArpcRequestData::TYPE_ID = "navi.arpc_request";

NaviArpcRequestData::NaviArpcRequestData(
        google::protobuf::RpcController *controller,
        const google::protobuf::Message *request,
        google::protobuf::Closure *closure,
        const std::string &methodName)
    : Data(TYPE_ID)
    , _controller(controller)
    , _request(request)
    , _closure(closure)
    , _methodName(methodName)
{
}

NaviArpcRequestData::~NaviArpcRequestData() {
}

const google::protobuf::Message *NaviArpcRequestData::getRequest() const {
    return _request;
}

float NaviArpcRequestData::degradeLevel(float level) const {
    auto gigClosure = dynamic_cast<multi_call::GigClosure *>(_closure);
    if (!gigClosure) {
        return multi_call::MIN_PERCENT;
    }
    return gigClosure->degradeLevel(level);
}

std::string NaviArpcRequestData::getClientIp() const {
    auto gigController =
        dynamic_cast<multi_call::GigRpcController *>(_controller);
    if (gigController) {
        auto protocolController = gigController->getProtocolController();
        auto anetRpcController =
            dynamic_cast<arpc::ANetRPCController *>(protocolController);
        if (anetRpcController) {
            return "arpc " + anetRpcController->GetClientAddress();
        } else {
            auto httpController =
                dynamic_cast<http_arpc::HTTPRPCControllerBase *>(
                    protocolController);
            if (httpController) {
                return "http " + httpController->GetAddr();
            }
        }
    }
    return "";
}

NaviArpcRequestType::NaviArpcRequestType()
    : Type(__FILE__, NaviArpcRequestData::TYPE_ID)
{
}

NaviArpcRequestType::~NaviArpcRequestType() {
}

REGISTER_TYPE(NaviArpcRequestType);

}

