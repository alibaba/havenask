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
#pragma once

#include <arpc/ANetRPCController.h>
#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "aios/network/http_arpc/HTTPRPCController.h"
#include "aios/network/http_arpc/HTTPRPCServerClosure.h"
#include "arpc/RPCServerClosure.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace admin {

template <typename REQUEST, typename RESPONSE>
class AdminLogClosure {
public:
    AdminLogClosure(const REQUEST *request,
                    const RESPONSE *response,
                    google::protobuf::Closure *done,
                    std::string methodName,
                    int32_t count = 100)
        : _request(request), _response(response), _done(done), _methodName(methodName), _count(count) {
        _beginTime = autil::TimeUtility::currentTime();
    }
    ~AdminLogClosure() {
        if (_request != NULL && _response != NULL) {
            doLog();
        }
        if (_done) {
            _done->Run();
        }
    }

private:
    AdminLogClosure(const AdminLogClosure &);
    AdminLogClosure &operator=(const AdminLogClosure &);

public:
    int64_t getBeginTime() { return _beginTime; }
    void doLog() {
        static int32_t count = 0;
        if (count <= 0) {
            doAccessLog();
            count = _count;
        }
        count--;
    }
    void doAccessLog() {
        int64_t usedTime = autil::TimeUtility::currentTime() - _beginTime;
        std::string clientAddress = getClientAddress();
        const protocol::ErrorInfo &ecInfo = _response->errorinfo();
        std::string ecName = protocol::ErrorCode_Name(ecInfo.errcode());
        AUTIL_LOG(INFO,
                  "[%ldus]-[%s]-[%s]-[%s]-[%s]-[%s]",
                  usedTime,
                  _methodName.c_str(),
                  ecName.c_str(),
                  clientAddress.c_str(),
                  _request->ShortDebugString().c_str(),
                  _response->ShortDebugString().c_str());
    }

    std::string getClientAddress() {
        std::string clientAddress;
        if (_done) {
            auto *rpcClosure = dynamic_cast<arpc::RPCServerClosure *>(_done);
            if (rpcClosure) {
                auto *rpcController = rpcClosure->GetRpcController();
                if (rpcController) {
                    auto *anetRpcController = dynamic_cast<arpc::ANetRPCController *>(rpcController);
                    if (anetRpcController) {
                        clientAddress = anetRpcController->GetClientAddress();
                    }
                }
            } else {
                auto *httpRpcClosure = dynamic_cast<http_arpc::HTTPRPCServerClosure *>(_done);
                if (httpRpcClosure) {
                    auto *httpRpcController = httpRpcClosure->getController();
                    if (httpRpcController) {
                        clientAddress = httpRpcController->GetAddr();
                    }
                }
            }
        }
        return clientAddress;
    }

private:
    const REQUEST *_request;
    const RESPONSE *_response;
    google::protobuf::Closure *_done;
    std::string _methodName;
    int32_t _count;
    int64_t _beginTime;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(swift.admin, AdminLogClosure, REQUEST, RESPONSE);

} // namespace admin
} // namespace swift
