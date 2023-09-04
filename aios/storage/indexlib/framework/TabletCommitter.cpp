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
#include "indexlib/framework/TabletCommitter.h"

#include <utility>

#include "ITabletImporter.h"
#include "autil/TimeUtility.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/framework/CommitOptions.h"
#include "indexlib/framework/IdGenerator.h"
#include "indexlib/framework/VersionCommitter.h"
#include "indexlib/framework/cleaner/DropIndexCleaner.h"
#include "indexlib/framework/index_task/Constant.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, TabletCommitter);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

TabletCommitter::TabletCommitter(const std::string& tabletName)
    : _tabletName(tabletName)
    , _lastCommitSegmentId(INVALID_SEGMENTID)
    , _dumpErrorStatus(Status::OK())
    , _sealed(false)
{
}

void TabletCommitter::Init(const std::shared_ptr<VersionMerger>& versionMerger,
                           const std::shared_ptr<TabletData>& tabletData)
{
    _versionMerger = versionMerger;
    const auto& onDiskVersion = tabletData->GetOnDiskVersion();
    _schemaRoadMap = onDiskVersion.GetSchemaVersionRoadMap();
    _lastCommitSegmentId = onDiskVersion.GetLastSegmentId();
    _lastPublicVersion = onDiskVersion;
}

bool TabletCommitter::NeedCommit() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    return !_commitQueue.empty() || !_dumpErrorStatus.IsOK() || (_versionMerger && _versionMerger->NeedCommit());
}

void TabletCommitter::Push(segmentid_t segId)
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (_sealed) {
        return;
    }
    _commitQueue.emplace_back(Operation::DumpOp(segId));
}

void TabletCommitter::Seal()
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (_sealed) {
        return;
    }
    _sealed = true;
    _commitQueue.emplace_back(Operation::SealOp());
}

void TabletCommitter::SetLastPublicVersion(const Version& version) { _lastPublicVersion = version; }

void TabletCommitter::SetSealed(bool sealed)
{
    std::lock_guard<std::mutex> guard(_mutex);
    _sealed = sealed;
}

void TabletCommitter::AlterTable(schemaid_t schemaId)
{
    std::lock_guard<std::mutex> guard(_mutex);
    _commitQueue.emplace_back(Operation::AlterTableOp(schemaId));
}

void TabletCommitter::Import(const std::vector<Version>& versions, const std::shared_ptr<ITabletImporter>& importer,
                             const ImportOptions& options)
{
    std::lock_guard<std::mutex> guard(_mutex);
    _commitQueue.emplace_back(Operation::ImportOp(versions, importer, options));
}

void TabletCommitter::HandleIndexTask(const std::string& taskType, const std::string& taskName,
                                      const std::map<std::string, std::string>& params, Action action,
                                      const std::string& comment)
{
    std::lock_guard<std::mutex> guard(_mutex);
    _commitQueue.emplace_back(Operation::IndexTaskOp(taskType, taskName, params, action, comment));
}

std::vector<std::shared_ptr<IndexTaskMeta>>
TabletCommitter::CalculateIndexTasks(const std::vector<std::shared_ptr<IndexTaskMeta>>& onDiskIndexTasks,
                                     const std::vector<std::shared_ptr<IndexTaskMeta>>& mergeVersionIndexTasks,
                                     int64_t currentTimeInSec) const
{
    std::map<std::pair<std::string, std::string>, std::shared_ptr<IndexTaskMeta>> stoppedTasks;
    for (const auto& task : mergeVersionIndexTasks) {
        if (task->state == IndexTaskMeta::DONE || task->state == IndexTaskMeta::ABORTED) {
            stoppedTasks[std::make_pair(task->taskType, task->taskName)] = task;
        }
    }
    std::vector<std::shared_ptr<IndexTaskMeta>> result;
    for (const auto& task : onDiskIndexTasks) {
        auto iter = stoppedTasks.find(std::make_pair(task->taskType, task->taskName));
        if (iter != stoppedTasks.end()) {
            const auto& stoppedTask = iter->second;
            if (currentTimeInSec - stoppedTask->endTimeInSecs < indexlib::DEFAULT_DONE_TASK_TTL_IN_SECONDS) {
                result.emplace_back(iter->second);
            } else {
                AUTIL_LOG(INFO, "task[%s][%s] exceeds stopped task ttl [%ld]", task->taskType.c_str(),
                          task->taskName.c_str(), indexlib::DEFAULT_DONE_TASK_TTL_IN_SECONDS);
            }
        } else {
            result.emplace_back(task);
        }
    }
    return result;
}

std::pair<Status, Version>
TabletCommitter::GenerateVersionWithoutId(const std::shared_ptr<TabletData>& tabletData, const Fence& fence,
                                          std::shared_ptr<VersionMerger::MergedVersionInfo> mergedVersionInfo,
                                          std::vector<std::string>& filteredDirs)
{
    decltype(_commitQueue) newCommitQueue;
    {
        std::lock_guard<std::mutex> guard(_mutex);
        newCommitQueue.swap(_commitQueue);
    }
    segmentid_t maxSegmentId = _lastCommitSegmentId;
    segmentid_t tempId = INVALID_SEGMENTID;
    std::vector<Version> importVersions;
    ImportOptions importOptions;
    std::shared_ptr<ITabletImporter> importer;
    std::vector<Operation> indexTaskOps;
    for (auto iter = newCommitQueue.begin(); iter != newCommitQueue.end(); ++iter) {
        const auto& op = *iter;
        if (op.IsDump()) {
            tempId = op.GetSegmentId();
        } else if (op.IsAlterTable()) {
            _schemaRoadMap.push_back(op.GetAlterSchemaId());
        }
        // TODO(yonghao.fyh): only lastest import is affective, consider multi call import
        else if (op.IsImport()) {
            importVersions = op.GetImportVersions();
            importer = op.GetImporter();
            importOptions = op.GetImportOptions();
        } else if (op.IsIndexTask()) {
            indexTaskOps.push_back(op);
        }
    }
    if (tempId != INVALID_SEGMENTID) {
        maxSegmentId = tempId;
    }
    auto segmentType =
        (maxSegmentId == INVALID_SEGMENTID) ? Segment::SegmentStatus::ST_BUILT : Segment::SegmentStatus::ST_UNSPECIFY;
    auto slice = tabletData->CreateSlice(segmentType);
    Version version;
    version.SetIndexTaskHistory(tabletData->GetOnDiskVersion().GetIndexTaskHistory());

    std::vector<std::shared_ptr<IndexTaskMeta>> mergedVersionIndexTasks;
    if (mergedVersionInfo) {
        auto onDiskVersion = tabletData->GetOnDiskVersion();
        // when full build, with tmp build root, base version empty
        if (onDiskVersion.GetVersionId() != INVALID_VERSIONID) {
            for (auto [segmentId, _] : mergedVersionInfo->baseVersion) {
                if (!tabletData->GetOnDiskVersion().HasSegment(segmentId)) {
                    TABLET_LOG(INFO, "segment [%d] of baseVersion[%d] is not in new version [%d]", segmentId,
                               mergedVersionInfo->baseVersion.GetVersionId(),
                               tabletData->GetOnDiskVersion().GetVersionId());
                    mergedVersionInfo.reset();
                    break;
                }
            }
        }
        if (mergedVersionInfo) {
            auto targetSchema = tabletData->GetTabletSchema(mergedVersionInfo->targetVersion.GetReadSchemaId());
            filteredDirs = DropIndexCleaner::CaclulateDropIndexDirs(tabletData, targetSchema);
            auto status = fence.GetFileSystem()
                              ->MountVersion(fence.GetGlobalRoot(), mergedVersionInfo->targetVersion.GetVersionId(),
                                             /*logicalPath=*/"", indexlib::file_system::FSMT_READ_ONLY, nullptr)
                              .Status();
            RETURN2_IF_STATUS_ERROR(status, framework::Version(), "mount version [%d] failed",
                                    mergedVersionInfo->targetVersion.GetVersionId());
            version = mergedVersionInfo->targetVersion.Clone();
            mergedVersionIndexTasks = version.GetIndexTasks();
        }
    }
    const auto& lastOnDiskVersion = tabletData->GetOnDiskVersion();

    std::vector<std::shared_ptr<IndexTaskMeta>> onDiskVersionIndexTasks = lastOnDiskVersion.GetIndexTasks();
    auto indexTasks = CalculateIndexTasks(onDiskVersionIndexTasks, mergedVersionIndexTasks);
    segmentid_t lastSegmentId = lastOnDiskVersion.GetLastSegmentId();
    if (version.GetLastSegmentId() < lastSegmentId) {
        version.SetLastSegmentId(lastSegmentId);
    }

    if (lastOnDiskVersion.GetReadSchemaId() > version.GetReadSchemaId()) {
        version.SetReadSchemaId(lastOnDiskVersion.GetReadSchemaId());
    }
    assert(_schemaRoadMap.size() > 0);
    version.SetSchemaId(_schemaRoadMap[_schemaRoadMap.size() - 1]);
    version.SetSchemaVersionRoadMap(_schemaRoadMap);
    version.SetFenceName(fence.GetFenceName());
    version.SetTabletName(_tabletName);
    // set base index tasks
    version.SetIndexTasks(indexTasks);
    // generate new index tasks
    for (const auto& op : indexTaskOps) {
        assert(op.IsIndexTask());
        if (!op.IsIndexTask()) {
            continue;
        }
        if (op.GetAction() == Action::ABORT) {
            version.UpdateIndexTaskState(op.GetTaskType(), op.GetTaskName(), IndexTaskMeta::ABORTED);
        } else if (op.GetAction() == Action::OVERWRITE) {
            auto indexTask = version.GetIndexTask(op.GetTaskType(), op.GetTaskName());
            if (indexTask == nullptr) {
                TABLET_LOG(WARN, "overwrite index task failed, task type %s, task name %s not found",
                           op.GetTaskType().c_str(), op.GetTaskName().c_str());
                continue;
            }
            auto params = op.GetParams();
            if (params.find(PARAM_LAST_SEQUENCE_NUMBER) == params.end()) {
                // reuse previous last sequence number to ensure data sequence is right
                auto iter = indexTask->params.find(PARAM_LAST_SEQUENCE_NUMBER);
                if (iter == indexTask->params.end()) {
                    TABLET_LOG(WARN,
                               "overwrite index task failed, last sequence number not found, task type %s, task "
                               "name %s",
                               op.GetTaskType().c_str(), op.GetTaskName().c_str());
                    continue;
                }
                params[PARAM_LAST_SEQUENCE_NUMBER] = iter->second;
            }
            version.OverwriteIndexTask(op.GetTaskType(), op.GetTaskName(), params);
        } else if (op.GetAction() == Action::SUSPEND) {
            auto indexTask = version.GetIndexTask(op.GetTaskType(), op.GetTaskName());
            if (indexTask != nullptr) {
                version.UpdateIndexTaskState(op.GetTaskType(), op.GetTaskName(), IndexTaskMeta::SUSPENDED);
            } else {
                version.AddIndexTask(op.GetTaskType(), op.GetTaskName(), op.GetParams(), IndexTaskMeta::SUSPENDED,
                                     op.GetComment());
            }
        } else if (op.GetAction() == Action::ADD) {
            version.AddIndexTask(op.GetTaskType(), op.GetTaskName(), op.GetParams(), IndexTaskMeta::READY,
                                 op.GetComment());
        } else {
            TABLET_LOG(WARN, "invalid action, usage action=%s|%s|%s|%s",
                       ActionConvertUtil::ActionToStr(Action::ADD).c_str(),
                       ActionConvertUtil::ActionToStr(Action::ABORT).c_str(),
                       ActionConvertUtil::ActionToStr(Action::OVERWRITE).c_str(),
                       ActionConvertUtil::ActionToStr(Action::SUSPEND).c_str());
            continue;
        }
    }
    std::shared_ptr<SegmentDescriptions> segDescrptions = version.GetSegmentDescriptions();
    assert(segDescrptions);

    auto newSegmentStats = segDescrptions->GetSegmentStatisticsVector();

    segmentid_t maxDumpingSegmentId = INVALID_SEGMENTID;
    for (auto seg : slice) {
        segmentid_t segmentId = seg->GetSegmentId();
        if (version.HasSegment(segmentId)) {
            continue;
        }
        if (maxSegmentId != INVALID_SEGMENTID && segmentId > maxSegmentId &&
            seg->GetSegmentStatus() != Segment::SegmentStatus::ST_BUILT) {
            // skip not dumpped segments.
            // if maxSegmentId is INVALID, slice contains only built segments
            continue;
        }
        if (mergedVersionInfo && mergedVersionInfo->baseVersion.HasSegment(segmentId)) {
            // covered by merged version
            continue;
        }
        if (seg->GetSegmentStatus() == Segment::SegmentStatus::ST_DUMPING && segmentId > maxDumpingSegmentId) {
            maxDumpingSegmentId = segmentId;
        }
        auto [status, segStat] = seg->GetSegmentInfo()->GetSegmentStatistics();
        RETURN2_IF_STATUS_ERROR(status, Version(), "get segment statistics failed");
        if (!segStat.empty()) {
            newSegmentStats.push_back(segStat);
        }
        version.AddSegment(segmentId);
        version.UpdateSegmentSchemaId(segmentId, seg->GetSegmentSchema()->GetSchemaId());

        if (segDescrptions->GetLevelInfo() == nullptr) {
            auto lastLevelInfo = lastOnDiskVersion.GetSegmentDescriptions()->GetLevelInfo();
            if (lastLevelInfo != nullptr) {
                auto levelInfo = std::make_shared<framework::LevelInfo>();
                levelInfo->Init(lastLevelInfo->GetTopology(), lastLevelInfo->GetLevelCount(),
                                lastLevelInfo->GetShardCount());
                segDescrptions->SetLevelInfo(levelInfo);
            } else {
                AUTIL_LOG(INFO, "init new level info failed as on disk level info not exist");
            }
        }
        seg->CollectSegmentDescription(segDescrptions);
    }
    segDescrptions->SetSegmentStatistics(newSegmentStats);

    if (maxDumpingSegmentId == INVALID_SEGMENTID) {
        // only inc segment in current tabletData, so get locator and ts from onDiskVersion
        auto onDiskVersion = tabletData->GetOnDiskVersion();
        if (onDiskVersion.GetVersionId() != INVALID_VERSIONID) {
            version.SetTimestamp(tabletData->GetOnDiskVersion().GetTimestamp());
            version.SetLocator(tabletData->GetOnDiskVersion().GetLocator());
        }
    } else {
        // get locator and ts from max dumping segment
        auto maxDumpingSegment = tabletData->GetSegment(maxDumpingSegmentId);
        assert(maxDumpingSegment != nullptr &&
               maxDumpingSegment->GetSegmentStatus() == Segment::SegmentStatus::ST_DUMPING);
        auto segmentInfo = maxDumpingSegment->GetSegmentInfo();
        version.SetTimestamp(segmentInfo->GetTimestamp());
        version.SetLocator(segmentInfo->GetLocator());
    }
    if (!importVersions.empty()) {
        if (importer == nullptr) {
            auto s = Status::InternalError("import is nullptr");
            TABLET_LOG(ERROR, "%s", s.ToString().c_str());
            return std::make_pair(s, Version());
        }
        auto status = importer->Import(importVersions, &fence, importOptions, &version);
        if (!status.IsOK() && !status.IsExist()) {
            return std::make_pair(status, Version());
        }
    }

    if (_sealed) {
        version.SetSealed();
    }
    _lastCommitSegmentId = maxSegmentId;
    if (!slice.empty() && version.GetSegmentCount() == 0) {
        TABLET_LOG(ERROR, "slice is not empty, but generated version has no segment");
        return std::make_pair(Status::InternalError(), Version());
    }
    return std::make_pair(Status::OK(), std::move(version));
}

std::pair<Status, Version> TabletCommitter::Commit(const std::shared_ptr<TabletData>& tabletData, const Fence& fence,
                                                   int32_t commitRetryCount, IdGenerator* idGenerator,
                                                   const CommitOptions& commitOptions)
{
    // 0. check dump error first
    {
        // TODO(zhenqi) optimize for segments already dump successful
        std::lock_guard<std::mutex> guard(_mutex);
        if (!_dumpErrorStatus.IsOK()) {
            TABLET_LOG(ERROR, "commit failed for dump error, status:[%s]", _dumpErrorStatus.ToString().c_str());
            return std::make_pair(_dumpErrorStatus, Version());
        }
    }
    std::shared_ptr<VersionMerger::MergedVersionInfo> mergedVersionInfo;
    if (_versionMerger) {
        mergedVersionInfo = _versionMerger->GetMergedVersionInfo();
    }
    versionid_t targetVersionId = INVALID_VERSIONID;
    if (commitOptions.GetTargetVersionId() != INVALID_VERSIONID) {
        auto status = idGenerator->UpdateBaseVersionId(commitOptions.GetTargetVersionId());
        if (status.IsOK()) {
            targetVersionId = commitOptions.GetTargetVersionId();
        } else {
            TABLET_LOG(ERROR, "commit with certain version id [%d] failed", commitOptions.GetTargetVersionId());
            return std::make_pair(status, Version());
        }
    }
    // 1. prepare version
    std::vector<std::string> filteredDirs;
    auto [status, version] = GenerateVersionWithoutId(tabletData, fence, mergedVersionInfo, filteredDirs);
    if (status.IsExist()) {
        status = Status::Abort();
    }
    if (!status.IsOK()) {
        // exist ->abort
        TABLET_LOG(ERROR, "prepare version failed, error[%s]", status.ToString().c_str());
        return std::make_pair(status, Version());
    }

    std::set<std::pair<std::string, std::string>> newDoneTasks;
    for (const auto& indexTask : version.GetIndexTasks()) {
        if (indexTask->state == IndexTaskMeta::DONE && indexTask->committedVersionId == INVALID_VERSIONID) {
            newDoneTasks.emplace(indexTask->taskType, indexTask->taskName);
        }
    }
    // 2. commit version
    bool commitSusccess = false;
    do {
        if (targetVersionId == INVALID_VERSIONID) {
            version.SetVersionId(idGenerator->GenerateVersionId());
        } else {
            version.SetVersionId(targetVersionId);
        }
        for (const auto& indexTask : version.GetIndexTasks()) {
            if (newDoneTasks.find(std::make_pair(indexTask->taskType, indexTask->taskName)) != newDoneTasks.end()) {
                indexTask->committedVersionId = version.GetVersionId();
            }
        }
        version.SetCommitTime(autil::TimeUtility::currentTime());
        const auto& versionDescs = commitOptions.GetVersionDescriptions();
        version.SetDescriptions(versionDescs);
        std::optional<Locator> specifyLocator = commitOptions.GetSpecificLocator();
        if (specifyLocator) {
            version.SetLocator(specifyLocator.value());
        }
        version.SetVersionLine(_lastPublicVersion.GetVersionLine());
        version.Finalize();
        if (commitOptions.NeedPublish()) {
            status = VersionCommitter::CommitAndPublish(version, fence, filteredDirs);
        } else {
            status = VersionCommitter::Commit(version, fence, filteredDirs);
        }
        if (status.IsOK()) {
            commitSusccess = true;
            break;
        } else if (status.IsExist()) {
            if (targetVersionId != INVALID_VERSIONID) {
                TABLET_LOG(WARN, "certain target version[%d] already exist, no retry.", targetVersionId);
                return std::make_pair(Status::Abort(), Version());
            }
            TABLET_LOG(WARN, "version[%d] exist, maybe retry", version.GetVersionId());
        } else {
            TABLET_LOG(ERROR, "commit version failed, error[%s]", status.ToString().c_str());
            return std::make_pair(status, Version());
        }
    } while (--commitRetryCount > 0);
    if (!commitSusccess) {
        TABLET_LOG(ERROR, "commit version failed after retry");
        return std::make_pair(Status::Abort("version exist"), Version());
    }
    auto [syncStatus, fsSyncFuture] = fence.GetFileSystem()->Sync(/*waitFinish=*/true).StatusWith();
    if (!syncStatus.IsOK()) {
        TABLET_LOG(ERROR, "file_system sync failed, error:%s", syncStatus.ToString().c_str());
        return std::make_pair(Status::IOError("file_system sync failed"), Version());
    }
    if (!fsSyncFuture.valid()) {
        TABLET_LOG(ERROR, "invalid shared_future get from LFS");
        return std::make_pair(Status::IOError("invalid shared_future get from LFS"), Version());
    }
    if (!fsSyncFuture.get()) {
        static const std::string errMsg("flush segment failed.");
        TABLET_LOG(ERROR, "%s.", errMsg.c_str());
        return std::make_pair(Status::IOError(errMsg.c_str()), Version());
    }
    if (mergedVersionInfo) {
        if (mergedVersionInfo->committedVersionId == INVALID_VERSIONID) {
            mergedVersionInfo->committedVersionId = version.GetVersionId();
            TABLET_LOG(INFO, "merge version [base: %d, target: %d] committed as version[%d]",
                       mergedVersionInfo->baseVersion.GetVersionId(), mergedVersionInfo->targetVersion.GetVersionId(),
                       version.GetVersionId());
        }
    }
    _lastPublicVersion = version;
    return std::make_pair(Status::OK(), std::move(version));
}

// Need switch leader when dumper error
void TabletCommitter::SetDumpError(Status dumpErrorStatus)
{
    std::lock_guard<std::mutex> guard(_mutex);
    _dumpErrorStatus = dumpErrorStatus;
}
Status TabletCommitter::GetDumpError() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    return _dumpErrorStatus;
}
#undef TABLET_LOG

} // namespace indexlibv2::framework
