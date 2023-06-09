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
#include "build_service/workflow/RealtimeBuilderImplBase.h"

#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/util/Monitor.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/RealTimeBuilderTaskItem.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;
using namespace indexlib::partition;
using namespace indexlib::util;

using namespace build_service::common;
using namespace build_service::config;
using namespace build_service::reader;
using namespace build_service::builder;
using namespace build_service::util;
using namespace build_service::proto;

using indexlib::util::TaskItem;
using indexlib::util::TaskItemPtr;
using indexlib::util::TaskScheduler;

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, RealtimeBuilderImplBase);

#define LOG_PREFIX _partitionId.buildid().ShortDebugString().c_str()

RealtimeBuilderImplBase::RealtimeBuilderImplBase(const string& configPath,
                                                 const indexlib::partition::IndexPartitionPtr& indexPart,
                                                 const RealtimeBuilderResource& builderResource, bool supportForceSeek)
    : _builder(NULL)
    , _configPath(configPath)
    , _indexPartition(indexPart)
    , _metricProvider(builderResource.metricProvider)
    , _isRecovered(false)
    , _autoSuspend(false)
    , _adminSuspend(false)
    , _startRecoverTime(-1)
    , _maxRecoverTime(600)
    , _recoverLatency(DEFAULT_RECOVER_LATENCY * 1000)
    , _timestampToSkip(-1)
    , _errorCode(ERROR_NONE)
    , _errorTime(0)
    , _buildCtrlTaskId(TaskScheduler::INVALID_TASK_ID)
    , _supportForceSeek(supportForceSeek)
    , _seekToLatestWhenRecoverFail(false)
    , _needForceSeek(false)
    , _forceSeekInfo(-1, -1)
    , _lostRt(nullptr)
    , _swiftClientCreator(builderResource.swiftClientCreator)
    , _buildFlowThreadResource(builderResource.buildFlowThreadResource)
    , _taskScheduler(builderResource.taskScheduler)
{
}

RealtimeBuilderImplBase::~RealtimeBuilderImplBase()
{
    if (_taskScheduler && _buildCtrlTaskId != TaskScheduler::INVALID_TASK_ID) {
        _taskScheduler->DeleteTask(_buildCtrlTaskId);
    }
}

bool RealtimeBuilderImplBase::start(const PartitionId& partitionId, KeyValueMap& kvMap)
{
    _partitionId = partitionId;
    BS_LOG(INFO, "build partition %s", partitionId.ShortDebugString().c_str());
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);

    _buildFlow.reset(createBuildFlow(_indexPartition, _swiftClientCreator));

    if (!prepareIntervalTask(resourceReader, partitionId, _metricProvider)) {
        return false;
    }

    return doStart(partitionId, kvMap);
}

bool RealtimeBuilderImplBase::prepareIntervalTask(const ResourceReaderPtr& resourceReader,
                                                  const PartitionId& partitionId, MetricProviderPtr metricProvider)
{
    ScopedLock lock(_realtimeMutex);

    string relativePath = ResourceReader::getClusterConfRelativePath(partitionId.clusternames(0));
    if (!resourceReader->getConfigWithJsonPath(relativePath, "build_option_config.max_recover_time", _maxRecoverTime)) {
        string errorMsg = "fail to read maxRecoverTime from config path " + relativePath;
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (_supportForceSeek) {
        if (!resourceReader->getConfigWithJsonPath(relativePath, "build_option_config.seek_to_latest_when_recover_fail",
                                                   _seekToLatestWhenRecoverFail)) {
            string errorMsg = "fail to read seek_to_latest_when_recover_fail from config path " + relativePath;
            BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        if (_seekToLatestWhenRecoverFail && metricProvider) {
            _lostRt = DECLARE_METRIC(metricProvider, "basic/lostRealtime", kmonitor::STATUS, "us");
        }
    }

    int64_t latencyInMs = DEFAULT_RECOVER_LATENCY;
    if (resourceReader->getConfigWithJsonPath(relativePath, "build_option_config.recover_delay_tolerance_ms",
                                              latencyInMs)) {
        if (latencyInMs < 0) {
            BS_PREFIX_LOG(ERROR,
                          "invalid recover latency [%ld], "
                          "it should be a positive integer",
                          latencyInMs);
        } else {
            _recoverLatency = latencyInMs * 1000;
        }
    }

    BS_PREFIX_LOG(INFO, "recover latency set to [%ld]", _recoverLatency);
    _startRecoverTime = TimeUtility::currentTimeInSeconds();
    setIsRecovered(false);

    assert(_taskScheduler);
    if (!_taskScheduler->DeclareTaskGroup("build_control", CONTROL_INTERVAL)) {
        return false;
    }

    TaskItemPtr taskItem(new RealTimeBuilderTaskItem(this));
    _buildCtrlTaskId = _taskScheduler->AddTask("build_control", taskItem);
    if (_buildCtrlTaskId == TaskScheduler::INVALID_TASK_ID) {
        BS_PREFIX_LOG(WARN, "create build control task failed");
        return false;
    }

    BS_PREFIX_LOG(INFO, "build control thread started");
    return true;
}

void RealtimeBuilderImplBase::executeBuildControlTask()
{
    if (_realtimeMutex.trylock() != 0) {
        return;
    }

    checkRecoverTime();
    if (_buildFlow && _buildFlow->hasFatalError()) {
        setErrorInfoUnsafe(ERROR_BUILD_DUMP, "build has fatal error");
    }
    if (needAutoSuspend()) {
        autoSuspend();
    } else if (needAutoResume()) {
        autoResume();
    }

    if (!producerAndBuilderPrepared()) {
        _realtimeMutex.unlock();
        return;
    }

    if (_seekToLatestWhenRecoverFail) {
        if (_needForceSeek) {
            if (seekProducerToLatest(_forceSeekInfo)) {
                _needForceSeek = false;
                setIsRecovered(true);
            }
        }
        checkForceSeek();
    }

    skipRtBeforeTimestamp();

    checkRecoverBuild();

    externalActions();
    _realtimeMutex.unlock();
}

void RealtimeBuilderImplBase::checkForceSeek()
{
    if (!_lostRt) {
        return;
    }

    int64_t incVersionTime = -1;
    auto ret = _builder->getIncVersionTimestampNonBlocking(incVersionTime);
    if (ret == Builder::RS_LOCK_BUSY) {
        return;
    }
    if (ret == Builder::RS_FAIL) {
        string errorMsg = "checkForceSeek failed";
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        return;
    }
    assert(ret == Builder::RS_OK);

    int64_t lost = _forceSeekInfo.second - std::max(incVersionTime, _forceSeekInfo.first);
    if (lost < 0) {
        lost = 0;
    }
    REPORT_METRIC(_lostRt, (double)lost);
}

void RealtimeBuilderImplBase::skipRtBeforeTimestamp()
{
    // TODO: refactor getLatestLOcator
    Locator latestLocator;
    if (!getLatestLocator(latestLocator)) {
        return;
    }

    if (latestLocator.GetOffset() >= _timestampToSkip) {
        return;
    }

    Locator locatorToJump(latestLocator.GetSrc(), _timestampToSkip);
    producerSeek(locatorToJump);
}

bool RealtimeBuilderImplBase::getLatestLocator(Locator& latestLocator)
{
    bool fromInc;
    return getLatestLocator(latestLocator, fromInc);
}

bool RealtimeBuilderImplBase::getLatestLocator(Locator& latestLocator, bool& fromInc)
{
    fromInc = false;
    Locator rtLocator;
    Builder::RET_STAT ret = _builder->getLastLocatorNonBlocking(rtLocator);
    if (ret == Builder::RS_LOCK_BUSY) {
        return false;
    }

    if (ret == Builder::RS_FAIL) {
        string errorMsg = "getLastLocator failed";
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    assert(ret == Builder::RS_OK);

    int64_t incVersionTime = -1;
    ret = _builder->getIncVersionTimestampNonBlocking(incVersionTime);
    if (ret == Builder::RS_LOCK_BUSY) {
        return false;
    }
    assert(ret == Builder::RS_OK);

    int64_t seekTs = OnlineJoinPolicy::GetRtSeekTimestamp(incVersionTime, rtLocator.GetOffset(), fromInc);

    if (fromInc) {
        latestLocator.SetSrc(rtLocator.GetSrc());
        latestLocator.SetOffset(seekTs);
        BS_INTERVAL_LOG2(120, INFO, "incVersionTime[%ld], rt locator[%s], inc covers rt", incVersionTime,
                         rtLocator.DebugString().c_str());
    } else {
        latestLocator = rtLocator;
    }
    return true;
}

void RealtimeBuilderImplBase::checkRecoverBuild()
{
    if (_isRecovered) {
        return;
    }
    int64_t maxTimestamp;
    if (!getLastTimestampInProducer(maxTimestamp)) {
        string errorMsg = "get last timestamp in producer failed";
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        return;
    }
    if (maxTimestamp == -1) {
        BS_PREFIX_LOG(INFO, "no realtime info found, set isRecoverd to be true.");
        setIsRecovered(true);
        return;
    }

    int64_t producerReadTimestamp;
    if (!getLastReadTimestampInProducer(producerReadTimestamp)) {
        string errorMsg = "get last read timestamp in producer failed";
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        return;
    }
    if (producerReadTimestamp + _recoverLatency >= maxTimestamp) {
        setIsRecovered(true);
        BS_PREFIX_LOG(
            INFO,
            "realtime build recover succeed. "
            "maxtimestamp_in_producer [%ld]us - locator_offset_in_producer [%ld]us = [%.2f]s < recover_latency [%.2f]s",
            producerReadTimestamp, _recoverLatency, (producerReadTimestamp - _recoverLatency) / 1000000.0,
            maxTimestamp / 1000000.0);
    }
}

void RealtimeBuilderImplBase::stop()
{
    BS_PREFIX_LOG(INFO, "stop realtime buildflow");
    _starter.stop();

    if (_buildCtrlTaskId != TaskScheduler::INVALID_TASK_ID) {
        _taskScheduler->DeleteTask(_buildCtrlTaskId);
        _buildCtrlTaskId = TaskScheduler::INVALID_TASK_ID;
    }

    if (_buildFlow.get()) {
        _buildFlow->stop();
    }
}

void RealtimeBuilderImplBase::suspendBuild()
{
    autil::ScopedLock lock(_realtimeMutex);
    _buildFlow->suspend();
    _adminSuspend = true;
}

void RealtimeBuilderImplBase::resumeBuild()
{
    autil::ScopedLock lock(_realtimeMutex);
    _adminSuspend = false;
    if (_autoSuspend) {
        string errorMsg = "RealtimeBuilderImpl resume fail! auto suspending!";
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        return;
    }
    _buildFlow->resume();
}

void RealtimeBuilderImplBase::setTimestampToSkip(int64_t timestamp)
{
    autil::ScopedLock lock(_realtimeMutex);
    if (_timestampToSkip != timestamp) {
        BS_PREFIX_LOG(INFO, "set timestampToSkip for [%s] as [%ld]", _partitionId.ShortDebugString().c_str(),
                      timestamp);
        _timestampToSkip = timestamp;
    }
}

void RealtimeBuilderImplBase::forceRecover()
{
    if (!_isRecovered) {
        BS_PREFIX_LOG(INFO, "force recover for partition [%s]", _partitionId.ShortDebugString().c_str());
        setIsRecovered(true);
    }
}

bool RealtimeBuilderImplBase::isRecovered() { return _isRecovered.load(); }

void RealtimeBuilderImplBase::getErrorInfo(RealtimeErrorCode& errorCode, string& errorMsg, int64_t& errorTime) const
{
    autil::ScopedLock lock(_realtimeMutex);
    errorCode = _errorCode;
    errorMsg = _errorMsg;
    errorTime = _errorTime;
}

bool RealtimeBuilderImplBase::needAutoSuspend()
{
    if (_autoSuspend) {
        return false;
    }

    IndexPartition::MemoryStatus memStatus = _indexPartition->CheckMemoryStatus();
    if (memStatus == IndexPartition::MS_REACH_TOTAL_MEM_LIMIT) {
        string errorMsg = "IndexPartition reach memoryUseLimit!";
        setErrorInfoUnsafe(ERROR_REOPEN_INDEX_LACKOFMEM_ERROR, errorMsg);
        return true;
    }

    if (memStatus == IndexPartition::MS_REACH_MAX_RT_INDEX_SIZE) {
        string errorMsg = "IndexPartition reach max rt index size!";
        setErrorInfoUnsafe(ERROR_BUILD_REALTIME_REACH_MAX_INDEX_SIZE, errorMsg);
        return true;
    }
    return false;
}

void RealtimeBuilderImplBase::autoSuspend()
{
    _buildFlow->suspend();
    _autoSuspend = true;
}

bool RealtimeBuilderImplBase::needAutoResume()
{
    if (!_autoSuspend) {
        return false;
    }
    return _indexPartition->CheckMemoryStatus() == IndexPartition::MS_OK;
}

void RealtimeBuilderImplBase::autoResume()
{
    _autoSuspend = false;
    if (!_adminSuspend) {
        _buildFlow->resume();
    }
}

BuildFlow* RealtimeBuilderImplBase::createBuildFlow(const indexlib::partition::IndexPartitionPtr& indexPartition,
                                                    const SwiftClientCreatorPtr& swiftClientCreator) const
{
    return new BuildFlow(swiftClientCreator, indexPartition->GetSchema(), _buildFlowThreadResource);
}

void RealtimeBuilderImplBase::setErrorInfoUnsafe(RealtimeErrorCode errorCode, const string& errorMsg)
{
    _errorCode = errorCode;
    _errorMsg = errorMsg;
    _errorTime = TimeUtility::currentTime();
}

void RealtimeBuilderImplBase::checkRecoverTime()
{
    if (_isRecovered || _needForceSeek) {
        return;
    }

    if (TimeUtility::currentTimeInSeconds() - _startRecoverTime > _maxRecoverTime) {
        string errorMsg = "recover takes too much time, treat it as recovered";
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        if (_seekToLatestWhenRecoverFail) {
            _needForceSeek = true;
        } else {
            setIsRecovered(true);
        }
        return;
    }
}

void RealtimeBuilderImplBase::setIsRecovered(bool isRecovered)
{
    if (!_isRecovered && isRecovered && _builder) {
        _builder->switchToConsistentMode();
    }
    _isRecovered = isRecovered;
}

#undef LOG_PREFIX

}} // namespace build_service::workflow
