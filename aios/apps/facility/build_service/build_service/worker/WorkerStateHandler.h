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

#include <deque>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/Lock.h"
#include "autil/StackTracer.h"
#include "autil/TimeUtility.h"
#include "autil/metric/ProcessCpu.h"
#include "autil/metric/ProcessMemory.h"
#include "beeper/beeper.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/CpuSpeedEstimater.h"
#include "build_service/common/Locator.h"
#include "build_service/common/NetworkTrafficEstimater.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/ResourceKeeper.h"
#include "build_service/common_define.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/task_base/RestartIntervalController.h"
#include "build_service/util/Monitor.h"
#include "build_service/workflow/BuildFlow.h"
#include "indexlib/base/Progress.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "kmonitor/client/MetricType.h"

namespace indexlib { namespace util {
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

#define FILL_ERRORINFO(errorCode, errorMsg, advice)                                                                    \
    do {                                                                                                               \
        ScopedLock lock(_lock);                                                                                        \
        _current.mutable_errorinfos()->Clear();                                                                        \
        proto::ErrorInfo* errorInfo = _current.add_errorinfos();                                                       \
        errorInfo->set_errorcode(errorCode);                                                                           \
        errorInfo->set_errormsg(errorMsg);                                                                             \
        errorInfo->set_advice(advice);                                                                                 \
        errorInfo->set_errortime(autil::TimeUtility::currentTimeInSeconds());                                          \
        beeper::EventTags tags;                                                                                        \
        proto::ProtoUtil::partitionIdToBeeperTags(_pid, tags);                                                         \
        tags.AddTag("errorCode", ProtoUtil::getServiceErrorCodeName(errorCode));                                       \
        tags.AddTag("errorAdvice", ProtoUtil::getErrorAdviceName(advice));                                             \
        BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, errorMsg, tags);                                                    \
        BS_LOG(ERROR, "%s", (string(errorMsg)).c_str());                                                               \
    } while (0)

#define FILL_COUNTER(oneCounter, metric)                                                                               \
    do {                                                                                                               \
        if (!metric) {                                                                                                 \
            break;                                                                                                     \
        }                                                                                                              \
        auto value = metric->getLastSampledValue().avg;                                                                \
        if (value != std::numeric_limits<double>::max()) {                                                             \
            counter.set_##oneCounter(value);                                                                           \
        }                                                                                                              \
    } while (0)

namespace build_service { namespace worker {

class WorkerStateHandler
{
public:
    WorkerStateHandler(const proto::PartitionId& pid, indexlib::util::MetricProviderPtr metricProvider,
                       const std::string& appZkRoot, const std::string& adminServiceName, const std::string& epochId)
        : _pid(pid)
        , _metricProvider(metricProvider)
        , _appZkRoot(appZkRoot)
        , _adminServiceName(adminServiceName)
        , _hasFatalError(false)
        , _isProcessing(false)
        , _isSuspended(false)
        , _configPath("")
        , _cpuEstimater(common::CpuSpeedEstimater(_metricProvider))
        , _networkEstimater(common::NetworkTrafficEstimater(_metricProvider))
        , _downloadConfigStartTs(0)
        , _initTimestamp(0)
        , _epochId(epochId)
    {
        DECLARE_STACK_TRACER_FILE("stack_tracer.log");
        _downLoadingConfigTime =
            DECLARE_METRIC(_metricProvider, "service/downLoadingConfigTime", kmonitor::GAUGE, "us");
        _waitingTargetTime = DECLARE_METRIC(_metricProvider, "service/waitingTargetTime", kmonitor::GAUGE, "us");
        _workerRunningTime = DECLARE_METRIC(_metricProvider, "service/workerRunningTime", kmonitor::GAUGE, "us");
        _cpuRatioMetric = DECLARE_METRIC(_metricProvider, "resource/cpuRatio", kmonitor::GAUGE, "%");
        _memRatioMetric = DECLARE_METRIC(_metricProvider, "resource/memRssRatio", kmonitor::GAUGE, "%");
        _initTimestamp = autil::TimeUtility::currentTime();
        _cpuQuota = autil::EnvUtil::getEnv("BS_ROLE_CPU_QUOTA", -1);
        _memQuota = autil::EnvUtil::getEnv("BS_ROLE_MEM_QUOTA", -1);
    }

    virtual ~WorkerStateHandler() {}

public:
    // if updated, skip PROTOCOL_VERSION=1, which has been used in a buggy released version
    static const int32_t PROTOCOL_VERSION = 2;
    WorkerStateHandler(const WorkerStateHandler&) = delete;
    WorkerStateHandler& operator=(const WorkerStateHandler&) = delete;

    virtual bool init() { return true; }
    void handleTargetState(const std::string& state, const std::string& targetResources)
    {
        if (isSuspended()) {
            return;
        }
        beginProcess();
        BS_MEMORY_BARRIER();
        bool resourceChanged = updateTargetResourceKeeperMap(targetResources);
        doHandleTargetState(state, resourceChanged);
        BS_MEMORY_BARRIER();
        endProcess();
    }
    virtual void doHandleTargetState(const std::string& state, bool hasResourceUpdated) = 0;
    virtual bool needSuspendTask(const std::string& state) = 0;
    virtual bool needRestartProcess(const std::string& state) { return false; }
    virtual void getCurrentState(std::string& state) = 0;
    virtual void doGetResourceKeeperMap(common::ResourceKeeperMap& usingKeepers) {}
    void getUsingResources(std::string& usingResourcesStr);

    virtual bool hasFatalError() = 0;
    bool isProcessing() const { return _isProcessing; }
    void setSuspendStatus(bool suspendStatus)
    {
        _isSuspended = suspendStatus;
        BS_MEMORY_BARRIER();
    }
    bool isSuspended() const { return _isSuspended; }
    static std::string staticDownloadConfig(const std::string& remoteConfig);

    void reportMetrics(bool hasTarget);

protected:
    bool readReservedClusterCheckpoints(const proto::PartitionId& pid, std::string& reservedCheckpointJsonStr);
    bool overWriteCounterConfig(const proto::PartitionId& pid, config::CounterConfig& counterConfig);

    common::ResourceKeeperMap getCurrentResourceKeeperMap();
    void beginProcess() { _isProcessing = true; }
    void endProcess() { _isProcessing = false; }
    void setFatalError();
    template <typename Current>
    void fillCurrentState(workflow::BuildFlow* buildFlow, Current& current);
    template <typename Current>
    void fillLocator(workflow::BuildFlow* buildFlow, Current& current);
    template <typename ErrorCollector>
    void fillErrorInfo(ErrorCollector* collector);
    template <typename Current>
    void fillProtocolVersion(Current& current);
    template <typename Current>
    void fillCpuSpeed(Current& current);
    template <typename Current>
    void fillNetworkTraffic(Current& current);
    template <typename Current>
    void saveCurrent(const Current& current, std::string& content);
    template <typename Current>
    void fillCurrentAddress(Current& current);
    std::string downloadConfig(const std::string& remoteConfig);

private:
    void cleanUselessErrorInfos();

protected:
    const proto::PartitionId _pid;
    indexlib::util::MetricProviderPtr _metricProvider;
    std::string _appZkRoot;
    std::string _adminServiceName;
    std::deque<proto::ErrorInfo> _errorInfos;
    volatile bool _hasFatalError;
    volatile bool _isProcessing;
    volatile bool _isSuspended;
    std::string _configPath;
    task_base::RestartIntervalControllerPtr _restartIntervalController;
    std::map<std::string, proto::WorkerNodeBase::ResourceInfo> _currentResource;
    autil::ThreadMutex _resourceLock;
    common::CpuSpeedEstimater _cpuEstimater;
    common::NetworkTrafficEstimater _networkEstimater;
    volatile int64_t _downloadConfigStartTs;
    indexlib::util::MetricPtr _downLoadingConfigTime;
    indexlib::util::MetricPtr _waitingTargetTime;
    indexlib::util::MetricPtr _workerRunningTime;
    indexlib::util::MetricPtr _cpuRatioMetric;
    indexlib::util::MetricPtr _memRatioMetric;

    autil::metric::ProcessCpu _cpu;
    autil::metric::ProcessMemory _mem;
    int64_t _cpuQuota = -1;
    int64_t _memQuota = -1;

    int64_t _initTimestamp;
    std::string _epochId;

private:
    template <typename Current>
    void fillWorkerStatus(workflow::BuildFlow* buildFlow, Current& current);

    /* if resources is diffrent from _currentResource, update currentResource & return true*/
    bool updateTargetResourceKeeperMap(const std::string& targetResourcesStr);

    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(WorkerStateHandler);

template <typename Current>
void WorkerStateHandler::fillLocator(workflow::BuildFlow* buildFlow, Current& current)
{
    if (!buildFlow) {
        return;
    }
    common::Locator locator;
    if (buildFlow->getLocator(locator)) {
        current.mutable_currentlocator()->set_sourcesignature(locator.GetSrc());
        current.mutable_currentlocator()->set_checkpoint(locator.GetOffset().first);
        current.mutable_currentlocator()->set_userdata(locator.GetUserData());
    } else {
        current.clear_currentlocator();
    }
}

template <typename Current>
void WorkerStateHandler::fillWorkerStatus(workflow::BuildFlow* buildFlow, Current& current)
{
    if (!buildFlow) {
        return;
    }
    if (buildFlow->isFinished()) {
        BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, "buildFlow stopped");
        current.set_status(proto::WS_STOPPED);
    } else {
        current.set_status(buildFlow->isStarted() ? proto::WS_STARTED : proto::WS_STARTING);
    }
}

template <typename ErrorCollector>
void WorkerStateHandler::fillErrorInfo(ErrorCollector* collector)
{
    if (!collector) {
        return;
    }
    collector->fillErrorInfos(_errorInfos);
    cleanUselessErrorInfos();
}

template <typename Current>
void WorkerStateHandler::fillProtocolVersion(Current& current)
{
    current.set_protocolversion(PROTOCOL_VERSION);
    current.set_configpath(_configPath);
}

template <typename Current>
void WorkerStateHandler::fillCurrentState(workflow::BuildFlow* buildFlow, Current& current)
{
    fillErrorInfo(buildFlow);
    current.clear_errorinfos();
    for (size_t i = 0; i < _errorInfos.size(); i++) {
        *current.add_errorinfos() = _errorInfos[i];
    }
    fillWorkerStatus(buildFlow, current);
}

template <typename Current>
void WorkerStateHandler::fillCpuSpeed(Current& current)
{
    current.mutable_machinestatus()->set_currentcpuspeed(_cpuEstimater.GetCurrentSpeed());
    current.mutable_machinestatus()->set_avgcpuspeed(_cpuEstimater.GetAvgSpeed());
    current.mutable_machinestatus()->set_mediancpuspeed(_cpuEstimater.GetMedianSpeed());
    current.mutable_machinestatus()->set_emacpuspeed(_cpuEstimater.GetEMASpeed());
    current.mutable_machinestatus()->set_emacpuspeednocoldstart(_cpuEstimater.GetEMASpeedNoColdStart());
}

template <typename Current>
void WorkerStateHandler::fillNetworkTraffic(Current& current)
{
    current.mutable_machinestatus()->set_byteinspeed(_networkEstimater.GetByteInSpeed());
    current.mutable_machinestatus()->set_byteoutspeed(_networkEstimater.GetByteOutSpeed());
    current.mutable_machinestatus()->set_pktinspeed(_networkEstimater.GetPktInSpeed());
    current.mutable_machinestatus()->set_pktoutspeed(_networkEstimater.GetPktOutSpeed());
    current.mutable_machinestatus()->set_pkterrspeed(_networkEstimater.GetPktErrSpeed());
    current.mutable_machinestatus()->set_pktdropspeed(_networkEstimater.GetPktDropSpeed());
}

template <typename Current>
void WorkerStateHandler::saveCurrent(const Current& current, std::string& content)
{
    content.clear();
    current.SerializeToString(&content);
    BS_INTERVAL_LOG(300, INFO, "current status: [%s]", current.ShortDebugString().c_str());
}

}} // namespace build_service::worker
