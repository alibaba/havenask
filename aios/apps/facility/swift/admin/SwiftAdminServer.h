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

#include <stdint.h>

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace google {
namespace protobuf {
class Closure;
class RpcController;
} // namespace protobuf
} // namespace google

namespace swift {
namespace config {
class AdminConfig;
} // namespace config
namespace monitor {
class AdminMetricsReporter;
} // namespace monitor
namespace protocol {
class SetLoggerLevelRequest;
class SetLoggerLevelResponse;
} // namespace protocol

namespace admin {
class SysController;

#define RPC_METHOD_DEFINE(method, requestType, responseType, count, slaveAllow, authCheckMethod)                       \
    void method(::google::protobuf::RpcController *controller,                                                         \
                const ::swift::protocol::requestType *request,                                                         \
                ::swift::protocol::responseType *response,                                                             \
                ::google::protobuf::Closure *done) override {                                                          \
        (void)controller;                                                                                              \
        swift::monitor::RpcCollector collector;                                                                        \
        swift::admin::AdminLogClosure<swift::protocol::requestType, swift::protocol::responseType> logClosure(         \
            request, response, done, #method, count);                                                                  \
        if (_sysController) {                                                                                          \
            arpc::RPCServerClosure *closure = dynamic_cast<arpc::RPCServerClosure *>(done);                            \
            if (closure) {                                                                                             \
                auto tracer = closure->GetTracer();                                                                    \
                if (tracer && (request->has_timeout() || request->timeout() > 0)) {                                    \
                    int64_t currentTime = autil::TimeUtility::currentTime();                                           \
                    int64_t startTime = tracer->GetBeginHandleRequest();                                               \
                    int64_t leftTime = request->timeout() * 1000 - currentTime + startTime;                            \
                    int64_t processTime = request->timeout() * _requestProcessTimeRatio * 10;                          \
                    if (leftTime < processTime) {                                                                      \
                        swift::protocol::ErrorInfo *ei = response->mutable_errorinfo();                                \
                        ei->set_errcode(swift::protocol::ERROR_ADMIN_WAIT_IN_QUEUE_TIMEOUT);                           \
                        ei->set_errmsg(ErrorCode_Name(ei->errcode()));                                                 \
                        collector.isTimeout = true;                                                                    \
                        AUTIL_LOG(ERROR,                                                                               \
                                  "ERROR_ADMIN_WAIT_IN_QUEUE_TIMEOUT, "                                                \
                                  "queueTime:[%ld], leftTime:[%ld], need processTime:[%ld],"                           \
                                  " request:[%s]",                                                                     \
                                  currentTime - startTime,                                                             \
                                  leftTime,                                                                            \
                                  processTime,                                                                         \
                                  request->ShortDebugString().c_str());                                                \
                        if (_metricsReporter) {                                                                        \
                            _metricsReporter->report##method##Metrics(collector);                                      \
                        }                                                                                              \
                        return;                                                                                        \
                    }                                                                                                  \
                }                                                                                                      \
            }                                                                                                          \
            auto authenticator = _sysController->getRequestAuthenticator();                                            \
            if (authenticator != nullptr && !authenticator->authCheckMethod(request)) {                                \
                swift::protocol::ErrorInfo *ei = response->mutable_errorinfo();                                        \
                if (request->has_versioninfo() && request->versioninfo().supportauthentication()) {                    \
                    ei->set_errcode(swift::protocol::ERROR_ADMIN_AUTHENTICATION_FAILED);                               \
                    ei->set_errmsg(ErrorCode_Name(ei->errcode()));                                                     \
                } else {                                                                                               \
                    ei->set_errcode(swift::protocol::ERROR_ADMIN_INVALID_PARAMETER);                                   \
                    ei->set_errmsg(ErrorCode_Name(ei->errcode()));                                                     \
                }                                                                                                      \
                collector.reject = true;                                                                               \
            } else {                                                                                                   \
                if (!_sysController->isMaster() && !slaveAllow) {                                                      \
                    swift::protocol::ErrorInfo *ei = response->mutable_errorinfo();                                    \
                    ei->set_errcode(swift::protocol::ERROR_ADMIN_NOT_LEADER);                                          \
                    ei->set_errmsg(ErrorCode_Name(ei->errcode()));                                                     \
                } else {                                                                                               \
                    _sysController->method(request, response);                                                         \
                }                                                                                                      \
            }                                                                                                          \
        } else {                                                                                                       \
            swift::protocol::ErrorInfo *ei = response->mutable_errorinfo();                                            \
            ei->set_errcode(swift::protocol::ERROR_ADMIN_NOT_LEADER);                                                  \
            ei->set_errmsg(ErrorCode_Name(ei->errcode()));                                                             \
        }                                                                                                              \
        if (_metricsReporter) {                                                                                        \
            collector.latency = autil::TimeUtility::currentTime() - logClosure.getBeginTime();                         \
            _metricsReporter->report##method##Metrics(collector);                                                      \
        }                                                                                                              \
    }

class SwiftAdminServer {
public:
    SwiftAdminServer(config::AdminConfig *config,
                     admin::SysController *sysController,
                     monitor::AdminMetricsReporter *reporter);
    virtual ~SwiftAdminServer() = 0;
    SwiftAdminServer(const SwiftAdminServer &) = delete;
    SwiftAdminServer &operator=(const SwiftAdminServer &) = delete;

public:
    bool init();
    void setLoggerLevel(::google::protobuf::RpcController *controller,
                        const ::swift::protocol::SetLoggerLevelRequest *request,
                        ::swift::protocol::SetLoggerLevelResponse *response,
                        ::google::protobuf::Closure *done);

protected:
    config::AdminConfig *_adminConfig = nullptr;
    admin::SysController *_sysController = nullptr;
    monitor::AdminMetricsReporter *_metricsReporter = nullptr;
    uint32_t _requestProcessTimeRatio;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftAdminServer);

} // namespace admin
} // namespace swift
