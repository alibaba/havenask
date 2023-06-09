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
#include "indexlib/framework/VersionMerger.h"

#include "autil/EnvUtil.h"
#include "autil/Scope.h"
#include "autil/TimeUtility.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskMetrics.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/util/EpochIdUtil.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, VersionMerger);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

namespace {
static const size_t SKIP_CLEAN_TASK =
    autil::EnvUtil::getEnv<size_t>("tablet_merge_skip_clean_task", /*defaultValue*/ 0);
}

VersionMerger::VersionMerger(std::string tabletName, std::string indexRoot,
                             std::shared_ptr<ITabletMergeController> controller,
                             std::unique_ptr<IIndexTaskPlanCreator> planCreator, MetricsManager* manager)
    : _tabletName(std::move(tabletName))
    , _indexRoot(std::move(indexRoot))
    , _controller(std::move(controller))
    , _planCreator(std::move(planCreator))
    , _lastProposedVersionId(INVALID_VERSIONID)
    , _recovered(false)
    , _stopped(false)
{
    RegisterMetrics(manager);
}

void VersionMerger::UpdateVersion(const Version& version)
{
    std::lock_guard<std::mutex> lock(_dataMutex);
    _currentBaseVersion = version;
    TABLET_LOG(INFO, "version [%d] updated", _currentBaseVersion.GetVersionId());
}

versionid_t VersionMerger::GetBaseVersion() const
{
    std::lock_guard<std::mutex> lock(_dataMutex);
    if (_mergedVersionInfo && (_mergedVersionInfo->committedVersionId == INVALID_VERSIONID ||
                               _currentBaseVersion.GetVersionId() < _mergedVersionInfo->committedVersionId)) {
        TABLET_LOG(
            INFO,
            "version [%d] would not propose, current merged base version[%d], target version[%d], comitted version[%d]",
            _currentBaseVersion.GetVersionId(), _mergedVersionInfo->baseVersion.GetVersionId(),
            _mergedVersionInfo->targetVersion.GetVersionId(), _mergedVersionInfo->committedVersionId);
        return INVALID_VERSIONID;
    }
    return _currentBaseVersion.GetVersionId();
}

future_lite::coro::Lazy<Status> VersionMerger::SubmitTask(IndexTaskContext* context)
{
    auto [status, plan] = _planCreator->CreateTaskPlan(context);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "create task plan failed: %s", status.ToString().c_str());
        co_return status;
    }
    if (!plan) {
        TABLET_LOG(INFO, "empty plan, do not merge");
        co_return Status::OK();
    }

    if (_taskMetrics) {
        _taskMetrics->SetCurrentTask(plan->GetTaskType(), plan->GetTaskName());
    }
    co_return co_await _controller->SubmitMergeTask(std::move(plan), context);
}

void VersionMerger::WaitStop()
{
    _controller->Stop();
    future_lite::coro::syncAwait([this]() -> future_lite::coro::Lazy<> {
        co_await _runMutex.coLock();
        _stopped = true;
        _runMutex.unlock();
        co_return;
    }());
}

void VersionMerger::FinishTask(versionid_t baseVersionId, bool removeTempFiles)
{
    TABLET_LOG(INFO, "version [%d] proposed", baseVersionId);
    if (baseVersionId != indexlibv2::INVALID_VERSIONID) {
        _lastProposedVersionId = baseVersionId;
    }
    if (SKIP_CLEAN_TASK) {
        TABLET_LOG(INFO, "clean task skipped");
        removeTempFiles = false;
    }
    if (_taskMetrics) {
        _taskMetrics->FinishCurrentTask();
    }
    [[maybe_unused]] auto status = _controller->CleanTask(removeTempFiles);
}

Status VersionMerger::FillMergedVersionInfo(const MergeTaskStatus& mergeTaskStatus)
{
    assert(mergeTaskStatus.code == MergeTaskStatus::DONE);

    auto info = std::make_shared<MergedVersionInfo>();
    info->baseVersion = mergeTaskStatus.baseVersion;
    info->targetVersion = mergeTaskStatus.targetVersion;
    TABLET_LOG(INFO, "finish merge result: base version[%d], target version[%d]", info->baseVersion.GetVersionId(),
               info->targetVersion.GetVersionId());
    {
        std::lock_guard<std::mutex> lock(_dataMutex);
        _mergedVersionInfo = std::move(info);
    }
    return Status::OK();
}

std::pair<Status, Version> VersionMerger::LoadVersion(versionid_t versionId) const
{
    if (versionId == INVALID_VERSIONID) {
        return {Status::InternalError("invalid merged version"), Version()};
    }
    auto versionRootDir = indexlib::file_system::Directory::GetPhysicalDirectory(_indexRoot);
    Version version;
    auto status = VersionLoader::GetVersion(versionRootDir, versionId, &version);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "load version [%d] from [%s] failed: %s", versionId, _indexRoot.c_str(),
                   status.ToString().c_str());
        return {status, std::move(version)};
    }
    return {Status::OK(), std::move(version)};
}

std::shared_ptr<VersionMerger::MergedVersionInfo> VersionMerger::GetMergedVersionInfo()
{
    std::lock_guard<std::mutex> lock(_dataMutex);
    return _mergedVersionInfo;
}

const std::shared_ptr<VersionMerger::MergedVersionInfo>& VersionMerger::GetMergedVersionInfo() const
{
    std::lock_guard<std::mutex> lock(_dataMutex);
    return _mergedVersionInfo;
}

bool VersionMerger::NeedCommit() const
{
    std::lock_guard<std::mutex> lock(_dataMutex);
    return _mergedVersionInfo && _mergedVersionInfo->committedVersionId == INVALID_VERSIONID;
}

future_lite::coro::Lazy<std::pair<Status, versionid_t>>
VersionMerger::ExecuteTask(const Version& sourceVersion, const std::string& taskType, const std::string& taskName,
                           const std::map<std::string, std::string>& params)
{
    co_return co_await InnerExecuteTask(sourceVersion, taskType, taskName, params);
}

future_lite::coro::Lazy<std::pair<Status, versionid_t>> VersionMerger::Run()
{
    std::string taskType;
    std::string taskName;
    IndexTaskContext::Parameters params;
    co_return co_await InnerExecuteTask(_currentBaseVersion, taskType, taskName, params);
}

future_lite::coro::Lazy<Status> VersionMerger::EnsureRecovered()
{
    if (!_recovered) {
        auto status = co_await _controller->Recover();
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "recover merge controller failed: %s", status.ToString().c_str());
            co_return Status::InternalError();
        }
        auto runningTask = _controller->GetRunningTaskStat();
        if (runningTask) {
            assert(runningTask->baseVersionId != INVALID_VERSIONID);
            auto [versionStat, baseVersion] = LoadVersion(runningTask->baseVersionId);
            if (!versionStat.IsOK()) {
                TABLET_LOG(ERROR, "load base version [%d] failed", runningTask->baseVersionId);
                co_return versionStat;
            }
            VersionCoord headVersionCoord(baseVersion.GetVersionId(), baseVersion.GetFenceName());
            TABLET_LOG(INFO, "recovered version [%d, %s], current version line [%s]", baseVersion.GetVersionId(),
                       baseVersion.GetFenceName().c_str(),
                       autil::legacy::ToJsonString(_currentBaseVersion.GetVersionLine(), true).c_str());
            if (!_currentBaseVersion.CanFastFowardFrom(headVersionCoord, /*hasBuildingSegment*/ false)) {
                TABLET_LOG(INFO, "running version [%d, %s] can't fastfowd to current version [%d], cancel it",
                           baseVersion.GetVersionId(), baseVersion.GetFenceName().c_str(),
                           _currentBaseVersion.GetVersionId());
                auto cancelStat = co_await _controller->CancelCurrentTask();
                if (!cancelStat.IsOK()) {
                    TABLET_LOG(ERROR, "cancel current task failed");
                    co_return cancelStat;
                }
            }
        }
        _recovered = true;
    }
    co_return Status::OK();
}

future_lite::coro::Lazy<std::pair<Status, versionid_t>>
VersionMerger::InnerExecuteTask(const Version& sourceVersion, const std::string& taskType, const std::string& taskName,
                                const std::map<std::string, std::string>& params)
{
    auto sourceVersionId = sourceVersion.GetVersionId();
    if (sourceVersionId != INVALID_VERSIONID && sourceVersion.GetVersionId() != _currentBaseVersion.GetVersionId()) {
        UpdateVersion(sourceVersion);
    }
    if (!_runMutex.tryLock()) {
        TABLET_LOG(WARN, "version merger is running or stopping");
        co_return std::make_pair(Status::InternalError(), INVALID_VERSIONID);
    }
    if (_stopped.load()) {
        TABLET_LOG(ERROR, "version merger is stopped");
        co_return std::make_pair(Status::InternalError(), INVALID_VERSIONID);
    }
    autil::ScopeGuard guard([this]() { _runMutex.unlock(); });
    auto recoverStatus = co_await EnsureRecovered();
    if (!recoverStatus.IsOK()) {
        TABLET_LOG(ERROR, "recover failed");
        co_return std::make_pair(recoverStatus, INVALID_VERSIONID);
    }
    if (!_controller->GetRunningTaskStat()) {
        versionid_t currentVersionId = GetBaseVersion();
        if (currentVersionId == INVALID_VERSIONID) {
            co_return std::make_pair(Status::InternalError(), INVALID_VERSIONID);
        }
        if (currentVersionId != _lastProposedVersionId) {
            TABLET_LOG(INFO, "version merger run for version [%d]", currentVersionId);
            auto context = _controller->CreateTaskContext(currentVersionId, taskType, taskName, params);
            if (!context) {
                TABLET_LOG(ERROR, "create context failed");
                co_return std::make_pair(Status::InternalError("create context failed"), INVALID_VERSIONID);
            }
            auto status = co_await SubmitTask(context.get());
            if (!status.IsOK()) {
                TABLET_LOG(ERROR, "run merge task for version [%d] failed: %s", currentVersionId,
                           status.ToString().c_str());
                FinishTask(INVALID_VERSIONID, /*removeTempFiles*/ false);
                co_return std::make_pair(status, INVALID_VERSIONID);
            }
            if (!_controller->GetRunningTaskStat()) {
                // empty plan
                TABLET_LOG(INFO, "no need merge for version [%d]", currentVersionId);
                FinishTask(currentVersionId, /*removeTempFiles*/ true);
                co_return std::make_pair(Status::OK(), INVALID_VERSIONID);
            }
        } else {
            // latest version already proposed.
            co_return std::make_pair(Status::OK(), INVALID_VERSIONID);
        }
    }
    auto [status, mergeTaskStatus] = co_await _controller->WaitMergeResult();
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "wait merge result failed: %s", status.ToString().c_str());
        co_return std::make_pair(Status::InternalError(), INVALID_VERSIONID);
    }

    auto baseVersionId = mergeTaskStatus.baseVersion.GetVersionId();
    auto targetVersionId = mergeTaskStatus.targetVersion.GetVersionId();
    if (mergeTaskStatus.code == MergeTaskStatus::ERROR) {
        TABLET_LOG(ERROR, "merge error for version [%d]", baseVersionId);
        FinishTask(INVALID_VERSIONID, /*removeTempFiles*/ false);
        co_return std::make_pair(Status::InternalError(), INVALID_VERSIONID);
    } else {
        auto status = FillMergedVersionInfo(mergeTaskStatus);
        if (status.IsOK()) {
            FinishTask(baseVersionId, /*removeTempFiles=*/true);
            co_return std::make_pair(status, targetVersionId);
        } else if (status.IsIOError()) {
            co_return std::make_pair(status, INVALID_VERSIONID);
        } else {
            FinishTask(INVALID_VERSIONID, /*removeTempFiles=*/false);
            co_return std::make_pair(status, INVALID_VERSIONID);
        }
    }
}

std::optional<ITabletMergeController::TaskStat> VersionMerger::GetRunningTaskStat() const
{
    return _controller->GetRunningTaskStat();
}

void VersionMerger::RegisterMetrics(MetricsManager* manager)
{
    if (!manager) {
        return;
    }
    _taskMetrics = std::dynamic_pointer_cast<IndexTaskMetrics>(
        manager->CreateMetrics("INDEX_TASK", [&]() -> std::shared_ptr<framework::IMetrics> {
            return std::make_shared<IndexTaskMetrics>(_tabletName, manager->GetMetricsReporter());
        }));
    assert(_taskMetrics);
}

void VersionMerger::UpdateMetrics(TabletData* tabletData)
{
    if (!_taskMetrics) {
        return;
    }

    auto lastMergeInfo = GetMergedVersionInfo();
    if (lastMergeInfo) {
        _taskMetrics->SetmergeBaseVersionIdValue(lastMergeInfo->baseVersion.GetVersionId());
        _taskMetrics->SetmergeTargetVersionIdValue(lastMergeInfo->targetVersion.GetVersionId());
        _taskMetrics->SetmergeCommittedVersionIdValue(lastMergeInfo->committedVersionId);
    }
    auto runningTaskStat = GetRunningTaskStat();
    if (runningTaskStat) {
        _taskMetrics->SetrunningMergeBaseVersionIdValue(runningTaskStat.value().baseVersionId);
        _taskMetrics->SetrunningMergeLeftOpsValue(runningTaskStat.value().totalOpCount -
                                                  runningTaskStat.value().finishedOpCount);
        _taskMetrics->SetrunningMergeTotalOpsValue(runningTaskStat.value().totalOpCount);
    } else {
        _taskMetrics->SetrunningMergeBaseVersionIdValue(INVALID_VERSIONID);
        _taskMetrics->SetrunningMergeLeftOpsValue(0);
        _taskMetrics->SetrunningMergeTotalOpsValue(0);
    }

    if (!tabletData) {
        return;
    }
    const auto& his = tabletData->GetOnDiskVersion().GetIndexTaskHistory();
    _taskMetrics->UpdateTaskHistory(his);
}

void VersionMerger::FillMetricsInfo(std::map<std::string, std::string>& infoMap)
{
    if (!_taskMetrics) {
        return;
    }
    _taskMetrics->FillMetricsInfo(infoMap);
}

#undef TABLET_LOG
} // namespace indexlibv2::framework
