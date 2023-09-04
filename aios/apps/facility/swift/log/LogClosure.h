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
#include <arpc/Tracer.h>
#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "arpc/ANetRPCController.h"
#include "arpc/RPCServerClosure.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/monitor/TimeTrigger.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/IpMapper.h"

namespace swift {
namespace util {

template <typename REQUEST, typename RESPONSE>
class LogClosure {
public:
    LogClosure(const REQUEST *request,
               RESPONSE *response,
               google::protobuf::Closure *done,
               const std::string &methodName)
        : _request(request), _response(response), _done(done), _methodName(methodName) {

        if (done) {
            arpc::RPCServerClosure *closure = dynamic_cast<arpc::RPCServerClosure *>(done);
            if (closure && closure->GetTracer()) {
                _queueTime = closure->GetTracer()->GetBeginHandleRequest();
            }
        }
        _beginTime = autil::TimeUtility::currentTime();
    }

    void logAndDone() {
        if (_autoDone) {
            if (_request != NULL && _response != NULL) {
                doLog();
            }
            reportMetrics();
            if (_done) {
                _done->Run();
            }
        }
    }

    virtual ~LogClosure() {
        logAndDone();
        _done = nullptr;
        _metricReporter = nullptr;
    }

    void Run() { delete this; }

    void setBrokerMetricReporter(monitor::BrokerMetricsReporter *metricReporter) { _metricReporter = metricReporter; }
    void setIpMapper(util::IpMapper *ipMapper) { _ipMapper = ipMapper; }

    void setLogInterval(int32_t count) { _count = count; }
    void setCloseForceLog(bool flag) { _closeForceLog = flag; }
    bool hasError() {
        auto ec = _response->errorinfo().errcode();
        if (ec == protocol::ERROR_BROKER_NO_DATA) {
            return false;
        }
        return protocol::ERROR_NONE != ec;
    }

private:
    LogClosure(const LogClosure &);
    LogClosure &operator=(const LogClosure &);

public:
    void setAutoDone(bool flag) { _autoDone = flag; }

private:
    void reportMetrics() {
        if (_metricReporter) {
            std::string drc = "unknown";
            if (_ipMapper) {
                const std::string &clientAddress = getClientAddress();
                drc = _ipMapper->mapIp(clientAddress);
            }
            const std::string &topicName = _request->topicname();
            int32_t partId = _request->partitionid();
            const protocol::ClientVersionInfo &versionInfo = _request->versioninfo();
            const std::string &clientVersion = versionInfo.version();
            const std::string &clientType = protocol::ClientType_Name(versionInfo.clienttype());
            monitor::AccessInfoCollectorPtr collector(
                new monitor::AccessInfoCollector(topicName, partId, clientVersion, clientType, drc, _methodName));
            collector->requestLength = _request->ByteSizeLong();
            collector->responseLength = _response->ByteSizeLong();
            _metricReporter->reportAccessInfoMetricsBackupThread(collector);
        }
    }

    void doLog() {
        static int32_t count = 0;
        if (hasError()) {
            doAccessLog();
        } else if (count <= 0 || (!_closeForceLog && _forceLog)) {
            doAccessLog();
            count = _count;
        }
        count--;
    }

public:
    virtual void doAccessLog() {
        int64_t curTime = autil::TimeUtility::currentTime();
        int64_t usedTime = curTime - _beginTime;
        int64_t totalTime = curTime - _queueTime;
        std::string clientAddress = getClientAddress();
        std::string topicName = _request->topicname();
        int32_t partId = _request->partitionid();
        const protocol::ErrorInfo &ecInfo = _response->errorinfo();
        protocol::ClientVersionInfo versionInfo = _request->versioninfo();
        std::string clientVersion = versionInfo.version();
        std::string clientType = protocol::ClientType_Name(versionInfo.clienttype());
        std::string ecName = protocol::ErrorCode_Name(ecInfo.errcode());
        AUTIL_LOG(INFO,
                  "[%ldus %ldus]-[%s]-[%s, %d]-[%s]-[%s, %s, %s]",
                  usedTime,
                  totalTime,
                  _methodName.c_str(),
                  topicName.c_str(),
                  partId,
                  ecName.c_str(),
                  clientAddress.c_str(),
                  clientType.c_str(),
                  clientVersion.c_str());
    }

    std::string getClientAddress() {
        std::string clientAddress;
        if (_done) {
            arpc::RPCServerClosure *rpcClosure = dynamic_cast<arpc::RPCServerClosure *>(_done);
            if (rpcClosure) {
                google::protobuf::RpcController *rpcController = rpcClosure->GetRpcController();
                if (rpcController) {
                    arpc::ANetRPCController *anetRpcController = dynamic_cast<arpc::ANetRPCController *>(rpcController);
                    if (anetRpcController) {
                        clientAddress = anetRpcController->GetClientAddress();
                    }
                }
            }
        }
        return clientAddress;
    }

public:
    const REQUEST *_request = nullptr;
    RESPONSE *_response = nullptr;
    google::protobuf::Closure *_done = nullptr;
    monitor::BrokerMetricsReporter *_metricReporter = nullptr;
    util::IpMapper *_ipMapper = nullptr;
    std::string _methodName;
    bool _autoDone = true;
    bool _closeForceLog = false;
    bool _forceLog = false;
    int32_t _count = 0;
    int64_t _beginTime = 0;
    int64_t _queueTime = 0;
    int32_t _longPollingEnqueueTimes = 0;
    int64_t _longPollingLatencyOnce = 0;
    int64_t _longPollingLatencyAcc = 0;
    monitor::TimeTrigger inQueueTimeTrigger;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(swift.util, LogClosure, REQUEST, RESPONSE);

} // namespace util
} // namespace swift
