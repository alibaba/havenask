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

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stddef.h>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "future_lite/coro/Lazy.h"
#include "future_lite/coro/Mutex.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/ITabletMergeController.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/index_task/IIndexTaskPlanCreator.h"
#include "indexlib/framework/index_task/MergeTaskDefine.h"

namespace indexlibv2::framework {
class IndexTaskMetrics;
class MetricsManager;

class VersionMerger
{
public:
    struct MergedVersionInfo {
        Version baseVersion;
        Version targetVersion;
        versionid_t committedVersionId = INVALID_VERSIONID;
    };

public:
    VersionMerger(std::string tabletName, std::string indexRoot, std::shared_ptr<ITabletMergeController> controller,
                  std::unique_ptr<IIndexTaskPlanCreator> planCreator, MetricsManager* manager, bool isOnline = false);

    void UpdateVersion(const Version& version);
    void UpdateCommittedVersionLocator(const Locator& locator);
    std::shared_ptr<MergedVersionInfo> GetMergedVersionInfo();
    const std::shared_ptr<MergedVersionInfo>& GetMergedVersionInfo() const;
    std::optional<ITabletMergeController::TaskStat> GetRunningTaskStat() const;
    bool NeedCommit() const;
    future_lite::coro::Lazy<std::pair<Status, versionid_t>>
    ExecuteTask(const Version& sourceVersion, const std::string& taskType, const std::string& taskName,
                const std::map<std::string, std::string>& params);
    future_lite::coro::Lazy<std::pair<Status, versionid_t>> Run();
    void WaitStop();

    void UpdateMetrics(TabletData* tabletData);
    void FillMetricsInfo(std::map<std::string, std::string>& infoMap);

private:
    future_lite::coro::Lazy<Status> EnsureRecovered();
    future_lite::coro::Lazy<std::pair<Status, versionid_t>>
    InnerExecuteTask(const Version& sourceVersion, const std::string& taskType, const std::string& taskName,
                     const std::map<std::string, std::string>& params);
    future_lite::coro::Lazy<Status> SubmitTask(IndexTaskContext* context);
    std::pair<Status, Version> LoadVersion(versionid_t versionId) const;
    void FinishTask(versionid_t baseVersionId, bool removeTempFiles);
    Status FillMergedVersionInfo(const MergeTaskStatus& mergeTaskStatus);
    versionid_t GetBaseVersion() const;
    int64_t GetCommittedVersionTimestamp() const;
    void RegisterMetrics(MetricsManager* manager);

private:
    std::string _tabletName;
    future_lite::coro::Mutex _runMutex;
    std::string _indexRoot;
    std::shared_ptr<ITabletMergeController> _controller;
    std::unique_ptr<IIndexTaskPlanCreator> _planCreator;
    std::shared_ptr<IndexTaskMetrics> _taskMetrics;
    mutable std::mutex _dataMutex;
    Version _currentBaseVersion;
    std::shared_ptr<MergedVersionInfo> _mergedVersionInfo;

    versionid_t _lastProposedVersionId;
    Locator _committedVersionLocator;
    bool _recovered;
    std::atomic<bool> _stopped;
    size_t _skipCleanTask = 0;
    bool _isOnline;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
