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
#include "navi/rpc_server/ArpcGraphClosure.h"
#include "navi/log/NaviLogger.h"
#include "navi/rpc_server/NaviArpcResponseData.h"
#include <multi_call/rpc/GigClosure.h>
#include <google/protobuf/message.h>

namespace navi {

ArpcGraphClosure::ArpcGraphClosure(const std::string &method,
                                   google::protobuf::RpcController *controller,
                                   google::protobuf::Message *response,
                                   google::protobuf::Closure *done)
    : _method(method)
    , _controller(controller)
    , _response(response)
    , _done(done)
{
}

ArpcGraphClosure::~ArpcGraphClosure() {
}

void ArpcGraphClosure::run(NaviUserResultPtr result) {
    if (!result) {
        std::string errorMsg("empty result");
        NAVI_KERNEL_LOG(ERROR, "run arpc graph failed: %s, method [%s]", errorMsg.c_str(), _method.c_str());
        _controller->SetFailed("run method " + _method +
                              " failed: " + errorMsg);
        _done->Run();
        delete this;
        return;
    }
    const auto &naviResult = result->getNaviResult();
    NaviUserData userData;
    bool eof = false;
    if (!result->nextData(userData, eof)) {
        std::string errorMsg("result has no data");
        errorMsg += ", navi result msg: " + naviResult->errorEvent.message;
        NAVI_KERNEL_LOG(ERROR, "run arpc graph failed: %s, method [%s]", errorMsg.c_str(), _method.c_str());
        _controller->SetFailed("run method " + _method +
                              " failed: " + errorMsg);
        _done->Run();
        delete this;
        return;
    }
    if (!eof) {
        std::string errorMsg(
            "result is data stream, which is not supported by arpc");
        errorMsg += ", navi result msg: " + naviResult->errorEvent.message;
        NAVI_KERNEL_LOG(ERROR, "run arpc graph failed: %s, method [%s]", errorMsg.c_str(), _method.c_str());
        _controller->SetFailed("run method " + _method +
                              " failed: " + errorMsg);
        _done->Run();
        delete this;
        return;
    }
    const auto &data = userData.data;
    if (!data) {
        std::string errorMsg("result data is null");
        errorMsg += ", navi result msg: " + naviResult->errorEvent.message;
        NAVI_KERNEL_LOG(ERROR, "run arpc graph failed: %s, method [%s]", errorMsg.c_str(), _method.c_str());
        _controller->SetFailed("run method " + _method +
                              " failed: " + errorMsg);
        _done->Run();
        delete this;
        return;
    }
    auto responseData = dynamic_cast<NaviArpcResponseData *>(data.get());
    if (!responseData) {
        std::string errorMsg("invalid result data type: " + data->getTypeId() +
                             "only support: " + NaviArpcResponseData::TYPE_ID);
        errorMsg += ", navi result msg: " + naviResult->errorEvent.message;
        NAVI_KERNEL_LOG(ERROR, "run arpc graph failed: %s, method [%s]", errorMsg.c_str(), _method.c_str());
        _controller->SetFailed("run method " + _method +
                              " failed: " + errorMsg);
        _done->Run();
        delete this;
        return;
    }
    std::unique_ptr<google::protobuf::Message> pbMsg(responseData->stealResponse());
    if (!pbMsg) {
        std::string errorMsg("null protobuf msg in response data");
        errorMsg += ", navi result msg: " + naviResult->errorEvent.message;
        NAVI_KERNEL_LOG(ERROR, "run arpc graph failed: %s, method [%s]", errorMsg.c_str(), _method.c_str());
        _controller->SetFailed("run method " + _method +
                              " failed: " + errorMsg);
        _done->Run();
        delete this;
        return;
    }
    auto responseRefl = _response->GetReflection();
    auto dataRefl = pbMsg->GetReflection();
    if (responseRefl != dataRefl) {
        const auto &responseTypeName = _response->GetDescriptor()->full_name();
        const auto &dataTypeName = pbMsg->GetDescriptor()->full_name();
        std::string errorMsg(
            "invalid protobuf msg format in response data, expect: " +
            responseTypeName + ", got: " + dataTypeName);
        errorMsg += ", navi result msg: " + naviResult->errorEvent.message;
        NAVI_KERNEL_LOG(ERROR, "run arpc graph failed: %s, method [%s]", errorMsg.c_str(), _method.c_str());
        _controller->SetFailed("run method " + _method +
                               " failed: " + errorMsg);
        _done->Run();
        delete this;
        return;
    }
    auto gigClosure = dynamic_cast<multi_call::GigClosure *>(_done);
    if (gigClosure) {
        auto ec = multi_call::MULTI_CALL_ERROR_NONE;
        if (EC_QUEUE_FULL == naviResult->ec) {
            ec = multi_call::MULTI_CALL_ERROR_DEC_WEIGHT;
        }
        gigClosure->setErrorCode(ec);
    }
    responseRefl->Swap(_response, pbMsg.get());
    _done->Run();
    pbMsg.reset();
    delete this;
}

}
