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
#include "build_service/workflow/RealtimeBuilderImplV2.h"

#include <assert.h>
#include <cstddef>

#include "alog/Logger.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/builder/BuildSpeedLimiter.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/workflow/BuildFlow.h"
#include "future_lite/NamedTaskScheduler.h"
#include "indexlib/base/Progress.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/CommitOptions.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/partition/builder_branch_hinter.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/TaskItem.h"
#include "indexlib/util/TaskScheduler.h"

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
BS_LOG_SETUP(workflow, RealtimeBuilderImplV2);

#define LOG_PREFIX _partitionId.buildid().ShortDebugString().c_str()

RealtimeBuilderImplV2::RealtimeBuilderImplV2(const string& configPath,
                                             std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                                             const RealtimeBuilderResource& builderResource,
                                             future_lite::NamedTaskScheduler* tasker)
    : _builder(NULL)
    , _configPath(configPath)
    , _tablet(std::move(tablet))
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
    , _buildCtrlTaskHandle(future_lite::TaskScheduler::INVALID_HANDLE)
    , _swiftClientCreator(builderResource.swiftClientCreator)
    , _buildFlowThreadResource(builderResource.buildFlowThreadResource)
    , _tasker(tasker)
{
}

RealtimeBuilderImplV2::~RealtimeBuilderImplV2() { stopTask(); }

bool RealtimeBuilderImplV2::start(const PartitionId& partitionId, KeyValueMap& kvMap)
{
    _partitionId = partitionId;
    BS_LOG(INFO, "build partition %s", partitionId.ShortDebugString().c_str());
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);

    _buildFlow.reset(createBuildFlow(_tablet.get(), _swiftClientCreator));

    if (!prepareIntervalTask(resourceReader, partitionId, _metricProvider)) {
        return false;
    }

    return doStart(partitionId, kvMap);
}

bool RealtimeBuilderImplV2::prepareIntervalTask(const ResourceReaderPtr& resourceReader, const PartitionId& partitionId,
                                                MetricProviderPtr metricProvider)
{
    std::lock_guard<std::mutex> lock(_realtimeMutex);

    string relativePath = ResourceReader::getClusterConfRelativePath(partitionId.clusternames(0));
    if (!resourceReader->getConfigWithJsonPath(relativePath, "build_option_config.max_recover_time", _maxRecoverTime)) {
        string errorMsg = "fail to read maxRecoverTime from config path " + relativePath;
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
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

    assert(_tasker);
    bool ret = _tasker->StartIntervalTask(
        "build_control", [this]() { executeBuildControlTask(); }, CONTROL_INTERVAL);
    if (!ret) {
        BS_PREFIX_LOG(ERROR, "create build control task failed");
        return false;
    }
    BS_PREFIX_LOG(INFO, "build control thread started");
    return true;
}

void RealtimeBuilderImplV2::executeBuildControlTask()
{
    std::unique_lock<std::mutex> lock(_realtimeMutex, std::defer_lock);
    if (!lock.try_lock()) {
        return;
    }

    if (_buildFlow and _buildFlow->needReconstruct()) {
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
        return;
    }

    skipRtBeforeTimestamp();

    checkRecoverBuild();

    externalActions();
}

void RealtimeBuilderImplV2::skipRtBeforeTimestamp()
{
    // TODO(tianxiao) check high bit
    auto latestLocator = getLatestLocator();
    if (latestLocator.GetOffset().first >= _timestampToSkip) {
        return;
    }
    common::Locator locatorToJump(latestLocator.GetSrc(), _timestampToSkip);
    producerSeek(locatorToJump);
}

indexlibv2::framework::Locator RealtimeBuilderImplV2::getLatestLocator() const
{
    auto latestLocator = _builder->getLastLocator();
    if (!latestLocator.IsValid()) {
        BS_PREFIX_LOG(WARN, "get invalid locator from builder");
        return indexlibv2::framework::Locator();
    }
    return latestLocator;
}

void RealtimeBuilderImplV2::checkRecoverBuild()
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
    // timstamp high bit
    if (!getLastReadTimestampInProducer(producerReadTimestamp)) {
        string errorMsg = "get last read timestamp in producer failed";
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        return;
    }
    if (producerReadTimestamp + _recoverLatency >= maxTimestamp) {
        setIsRecovered(true);
        BS_PREFIX_LOG(INFO,
                      "realtime build recover succeed. locator offset in producer:%ld,"
                      "maxtimestamp in producer: %ld",
                      producerReadTimestamp, maxTimestamp);
        return;
    }
}

void RealtimeBuilderImplV2::stopTask()
{
    if (_tasker) {
        if (_tasker->HasTask("build_control")) {
            [[maybe_unused]] bool ret = _tasker->DeleteTask("build_control");
            assert(ret);
        }
    }
}

void RealtimeBuilderImplV2::stop(StopOption stopOption)
{
    BS_PREFIX_LOG(INFO, "stop realtime buildflow");
    _starter.stop();
    stopTask();
    if (_buildFlow.get()) {
        _buildFlow->stop(stopOption);
    }
}

void RealtimeBuilderImplV2::suspendBuild()
{
    std::lock_guard<std::mutex> lock(_realtimeMutex);
    _buildFlow->suspend();
    _adminSuspend = true;
}

void RealtimeBuilderImplV2::resumeBuild()
{
    std::lock_guard<std::mutex> lock(_realtimeMutex);
    _adminSuspend = false;
    if (_autoSuspend) {
        string errorMsg = "RealtimeBuilderImpl resume fail! auto suspending!";
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        return;
    }
    _buildFlow->resume();
}

void RealtimeBuilderImplV2::setTimestampToSkip(int64_t timestamp)
{
    std::lock_guard<std::mutex> lock(_realtimeMutex);
    if (_timestampToSkip != timestamp) {
        BS_PREFIX_LOG(INFO, "set timestampToSkip for [%s] as [%ld]", _partitionId.ShortDebugString().c_str(),
                      timestamp);
        _timestampToSkip = timestamp;
    }
}

void RealtimeBuilderImplV2::forceRecover()
{
    if (!_isRecovered) {
        BS_PREFIX_LOG(INFO, "force recover for partition [%s]", _partitionId.ShortDebugString().c_str());
        setIsRecovered(true);
    }
}

bool RealtimeBuilderImplV2::isRecovered() { return _isRecovered.load(); }

bool RealtimeBuilderImplV2::needReconstruct() { return _buildFlow->needReconstruct(); }
bool RealtimeBuilderImplV2::hasFatalError() { return _buildFlow->hasFatalError(); }
bool RealtimeBuilderImplV2::isFinished()
{
    if (_buildFlow) {
        return _buildFlow->isFinished();
    }
    return false;
}

void RealtimeBuilderImplV2::getErrorInfo(RealtimeErrorCode& errorCode, string& errorMsg, int64_t& errorTime) const
{
    std::lock_guard<std::mutex> lock(_realtimeMutex);
    errorCode = _errorCode;
    errorMsg = _errorMsg;
    errorTime = _errorTime;
}

bool RealtimeBuilderImplV2::needAutoSuspend()
{
    if (_autoSuspend) {
        return false;
    }

    auto memStatus = _tablet->GetTabletInfos()->GetMemoryStatus();
    if (memStatus == indexlibv2::framework::MemoryStatus::REACH_TOTAL_MEM_LIMIT) {
        string errorMsg = "IndexPartition reach memoryUseLimit!";
        setErrorInfoUnsafe(ERROR_REOPEN_INDEX_LACKOFMEM_ERROR, errorMsg);
        return true;
    }

    if (memStatus == indexlibv2::framework::MemoryStatus::REACH_MAX_RT_INDEX_SIZE) {
        string errorMsg = "IndexPartition reach max rt index size!";
        setErrorInfoUnsafe(ERROR_BUILD_REALTIME_REACH_MAX_INDEX_SIZE, errorMsg);
        return true;
    }
    return false;
}

void RealtimeBuilderImplV2::autoSuspend()
{
    _buildFlow->suspend();
    _autoSuspend = true;
}

bool RealtimeBuilderImplV2::needAutoResume()
{
    if (!_autoSuspend) {
        return false;
    }
    return _tablet->GetTabletInfos()->GetMemoryStatus() == indexlibv2::framework::MemoryStatus::OK;
}

void RealtimeBuilderImplV2::autoResume()
{
    _autoSuspend = false;
    if (!_adminSuspend) {
        _buildFlow->resume();
    }
}

BuildFlow* RealtimeBuilderImplV2::createBuildFlow(indexlibv2::framework::ITablet* tablet,
                                                  const SwiftClientCreatorPtr& swiftClientCreator) const
{
    return new BuildFlow(tablet->GetTabletSchema(), _buildFlowThreadResource);
}

void RealtimeBuilderImplV2::setErrorInfoUnsafe(RealtimeErrorCode errorCode, const string& errorMsg)
{
    _errorCode = errorCode;
    _errorMsg = errorMsg;
    _errorTime = TimeUtility::currentTime();
}

void RealtimeBuilderImplV2::checkRecoverTime()
{
    if (_isRecovered) {
        return;
    }
    if (TimeUtility::currentTimeInSeconds() - _startRecoverTime > _maxRecoverTime) {
        string errorMsg = "recover takes too much time, treat it as recovered";
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        handleRecoverTimeout();
        setIsRecovered(true);
        return;
    }
}

bool RealtimeBuilderImplV2::needCommit() const { return _tablet->NeedCommit(); }
std::pair<indexlib::Status, indexlibv2::framework::VersionMeta> RealtimeBuilderImplV2::commit()
{
    bool needReopen = _tablet->GetTabletOptions()->FlushLocal();
    auto commitOptions = indexlibv2::framework::CommitOptions().SetNeedPublish(true).SetNeedReopenInCommit(needReopen);
    commitOptions.AddVersionDescription("generation",
                                        autil::StringUtil::toString(_partitionId.buildid().generationid()));
    return _tablet->Commit(commitOptions);
}

void RealtimeBuilderImplV2::setIsRecovered(bool isRecovered)
{
    if (!_isRecovered && isRecovered && _builder) {
        _builder->switchToConsistentMode();
    }
    _isRecovered = isRecovered;
}

#undef LOG_PREFIX

}} // namespace build_service::workflow
