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
#ifndef ISEARCH_MULTI_CALL_GIGRPCCONTROLLER_H
#define ISEARCH_MULTI_CALL_GIGRPCCONTROLLER_H

#include "aios/network/gig/multi_call/common/common.h"
#include "google/protobuf/service.h"

namespace multi_call {

class GigRpcController : public google::protobuf::RpcController
{
public:
    GigRpcController() : _ec(MULTI_CALL_ERROR_NO_RESPONSE), _rpcController(nullptr) {
    }
    ~GigRpcController() {
    }

private:
    GigRpcController(const GigRpcController &);
    GigRpcController &operator=(const GigRpcController &);

public:
    void Reset() override {
        if (_rpcController) {
            _rpcController->Reset();
        }
        _ec = MULTI_CALL_ERROR_NO_RESPONSE;
        _reason.clear();
    }
    bool Failed() const override {
        return _ec > MULTI_CALL_ERROR_DEC_WEIGHT;
    }
    std::string ErrorText() const override {
        return _reason;
    }
    void StartCancel() override {
        if (_rpcController) {
            _rpcController->StartCancel();
        }
    }
    void SetFailed(const std::string &reason) override {
        if (_rpcController) {
            _rpcController->SetFailed(reason);
        }
        _reason = reason;
        _ec = MULTI_CALL_ERROR_RPC_FAILED;
    }
    bool IsCanceled() const override {
        if (_rpcController) {
            return _rpcController->IsCanceled();
        } else {
            return false;
        }
    }
    void NotifyOnCancel(google::protobuf::Closure *callback) override {
        if (_rpcController) {
            _rpcController->NotifyOnCancel(callback);
        }
    }
    google::protobuf::RpcController *getProtocolController() const {
        return _rpcController;
    }

public:
    MultiCallErrorCode getErrorCode() const {
        return _ec;
    }
    void setErrorCode(MultiCallErrorCode ec) {
        _ec = ec;
    }
    void setRpcController(google::protobuf::RpcController *rpcController) {
        _rpcController = rpcController;
    }
    google::protobuf::RpcController *getRpcController() {
        return _rpcController;
    }
    std::string getClientIp() const;
private:
    MultiCallErrorCode _ec;
    std::string _reason;
    google::protobuf::RpcController *_rpcController;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigRpcController);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGRPCCONTROLLER_H
