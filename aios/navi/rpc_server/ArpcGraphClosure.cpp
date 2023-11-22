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
#include "navi/rpc_server/NaviDebugProtoJsonizer.h"
#include "navi/rpc_server/NaviStreamProtoJsonizer.h"
#include <multi_call/rpc/GigClosure.h>
#include <multi_call/arpc/CommonClosure.h>
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

void ArpcGraphClosure::init(std::string &debugParamStr) {
    auto commonClosure = dynamic_cast<multi_call::CommonClosure *>(_done);
    if (!commonClosure) {
        return;
    }
    auto httpClosure = dynamic_cast<http_arpc::HTTPRPCServerClosure *>(
        commonClosure->getClosure());
    if (!httpClosure) {
        return;
    }
    _httpClosure = httpClosure;
    debugParamStr = httpClosure->getAiosDebugType();
}

void ArpcGraphClosure::run(NaviUserResultPtr result) {
    std::string errorMsg;
    if (!doRun(result, errorMsg)) {
        if (!needReplace()) {
            _controller->SetFailed(errorMsg);
        }
    }
    _done->Run();
    delete this;
}

bool ArpcGraphClosure::doRun(const NaviUserResultPtr &result,
                             std::string &errorMsg) const
{
    auto success = processResult(result);
    if (!success) {
        result->updateError(EC_ARPC_RESULT);
    }
    auto naviResult = result->getNaviResult();
    if (!naviResult) {
        errorMsg = "run method " + _method + " failed: get navi result failed";
        return false;
    }
    if (needReplace()) {
        replaceProtoJsonizer(naviResult);
    }
    if (!success) {
        errorMsg = "run method " + _method + " failed, " + ", navi error msg: ";
        const auto &errorEvent = naviResult->getErrorEvent();
        if (errorEvent)  {
            errorMsg += errorEvent->message;
        }
        return false;
    }
    auto gigClosure = dynamic_cast<multi_call::GigClosure *>(_done);
    if (gigClosure) {
        auto ec = multi_call::MULTI_CALL_ERROR_NONE;
        if (EC_QUEUE_FULL == naviResult->getErrorCode()) {
            ec = multi_call::MULTI_CALL_ERROR_DEC_WEIGHT;
        }
        gigClosure->setErrorCode(ec);
    }
    return true;
}

bool ArpcGraphClosure::processResult(const NaviUserResultPtr &result) const {
    if (!result) {
        NAVI_KERNEL_LOG(ERROR,
                        "run arpc graph failed, empty result, method [%s]",
                        _method.c_str());
        return false;
    }
    bool streamMode = false;
    while (true) {
        NaviUserData userData;
        bool eof = false;
        if (!result->nextData(userData, eof)) {
            NAVI_KERNEL_LOG(
                ERROR, "run arpc graph failed, result has no data, method [%s]",
                _method.c_str());
            return false;
        }
        if (!eof) {
            streamMode = true;
        }
        if (!processData(streamMode, userData, eof)) {
            return false;
        }
        if (eof) {
            break;
        }
    }
    return true;
}

bool ArpcGraphClosure::processData(bool streamMode,
                                   const NaviUserData &userData,
                                   bool eof) const {
    if (streamMode) {
        return processStreamData(userData, eof);
    } else {
        std::unique_ptr<google::protobuf::Message> pbMsg(getDataMessage(userData.data));
        if (pbMsg) {
            auto responseRefl = _response->GetReflection();
            auto dataRefl = pbMsg->GetReflection();
            if (responseRefl != dataRefl) {
                const auto &responseTypeName =
                    _response->GetDescriptor()->full_name();
                const auto &dataTypeName = pbMsg->GetDescriptor()->full_name();
                NAVI_KERNEL_LOG(
                    ERROR,
                    "run arpc graph failed, invalid protobuf msg format in "
                    "response data, expect [%s], got [%s], method [%s]",
                    responseTypeName.c_str(), dataTypeName.c_str(),
                    _method.c_str());
                return false;
            }
            responseRefl->Swap(_response, pbMsg.get());
            return true;
        } else {
            return false;
        }
    }
}

bool ArpcGraphClosure::processStreamData(const NaviUserData &userData,
                                         bool eof) const
{
    if (!_httpClosure) {
        NAVI_KERNEL_LOG(ERROR, "not http protocol, can not process stream data");
        return false;
    }
    auto protoJsonizer = _httpClosure->getProtoJsonizer();
    auto streamProtoJsonizer =
        dynamic_cast<NaviStreamProtoJsonizer *>(protoJsonizer.get());
    if (!streamProtoJsonizer) {
        NAVI_KERNEL_LOG(ERROR,
                        "result is data stream, which is not supported by this "
                        "proto jsonizer, method [%s]",
                        _method.c_str());
        return false;
    }
    std::unique_ptr<google::protobuf::Message> pbMsg(
        getDataMessage(userData.data));
    if (pbMsg) {
        if (_response->GetReflection() != pbMsg->GetReflection()) {
            const auto &responseTypeName =
                _response->GetDescriptor()->full_name();
            const auto &dataTypeName = pbMsg->GetDescriptor()->full_name();
            NAVI_KERNEL_LOG(
                ERROR,
                "run arpc graph failed, invalid protobuf msg format in "
                "response data, expect [%s], got [%s], method [%s]",
                responseTypeName.c_str(), dataTypeName.c_str(),
                _method.c_str());
            return false;
        }
        if (!streamProtoJsonizer->collect(_response, userData, eof,
                                          pbMsg.release()))
        {
            NAVI_KERNEL_LOG(
                ERROR,
                "StreamProtoJsonizer collect message failed, method [%s]",
                _method.c_str());
            return false;
        }
        return true;
    } else {
        return false;
    }
}

google::protobuf::Message *
ArpcGraphClosure::getDataMessage(const DataPtr &data) const {
    if (!data) {
        NAVI_KERNEL_LOG(
            ERROR, "run arpc graph failed: result data is null, method [%s]",
            _method.c_str());
        return nullptr;
    }
    auto responseData = dynamic_cast<NaviArpcResponseData *>(data.get());
    if (!responseData) {
        NAVI_KERNEL_LOG(ERROR,
                        "run arpc graph failed, invalid result data type [%s], "
                        "only support [%s], method [%s]",
                        data->getTypeId().c_str(),
                        NaviArpcResponseData::TYPE_ID.c_str(), _method.c_str());
        return nullptr;
    }
    auto pbMsg = responseData->stealResponse();
    if (!pbMsg) {
        NAVI_KERNEL_LOG(ERROR,
                        "run arpc graph failed, null protobuf msg in response "
                        "data, method [%s]",
                        _method.c_str());
        return nullptr;
    }
    return pbMsg;
}

bool ArpcGraphClosure::needReplace() const {
    return (_needDebug && _httpClosure);
}

void ArpcGraphClosure::replaceProtoJsonizer(const NaviResultPtr &naviResult) const {
    auto visDef = naviResult->getVisProto();
    auto oldProtoJsonizer = _httpClosure->getProtoJsonizer();
    if (visDef && oldProtoJsonizer) {
        auto protoJsonizer =
            std::make_shared<NaviDebugProtoJsonizer>(visDef, oldProtoJsonizer);
        _httpClosure->setProtoJsonizer(protoJsonizer);
    }
}

}
