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

#include <memory>
#include <optional>

#include "autil/Log.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/ITabletMergeController.h"
#include "indexlib/framework/index_task/IIndexTaskPlanCreator.h"
#include "indexlib/framework/index_task/LocalExecuteEngine.h"
#include "indexlib/util/Clock.h"

namespace future_lite {
class Executor;
}

namespace indexlibv2 {
class MemoryQuotaController;
}

namespace indexlibv2::config {
class TabletSchema;
class TabletOptions;
} // namespace indexlibv2::config

namespace indexlibv2::table {

class LocalTabletMergeController : public framework::ITabletMergeController
{
    using TaskStat = ITabletMergeController::TaskStat;

public:
    struct InitParam {
        future_lite::Executor* executor = nullptr;
        std::shared_ptr<config::TabletSchema> schema;
        std::shared_ptr<config::TabletOptions> options;
        std::shared_ptr<MemoryQuotaController> memoryQuotaController;
        std::string partitionIndexRoot;
        std::string buildTempIndexRoot;
        std::shared_ptr<indexlib::util::MetricProvider> metricProvider;
        uint64_t branchId = 0;
    };

public:
    LocalTabletMergeController() = default;
    ~LocalTabletMergeController() override = default;

    Status Init(InitParam param);

public:
    future_lite::coro::Lazy<Status> Recover() override { co_return Status::OK(); }
    std::optional<TaskStat> GetRunningTaskStat() const override;
    future_lite::coro::Lazy<std::pair<Status, versionid_t>> GetLastMergeTaskResult() override;
    std::unique_ptr<framework::IndexTaskContext>
    CreateTaskContext(versionid_t baseVersionId, const std::string& taskType, const std::string& taskName,
                      const std::map<std::string, std::string>& params) override;
    future_lite::coro::Lazy<Status> SubmitMergeTask(std::unique_ptr<framework::IndexTaskPlan> plan,
                                                    framework::IndexTaskContext* context) override;
    future_lite::coro::Lazy<std::pair<Status, framework::MergeTaskStatus>> WaitMergeResult() override;
    future_lite::coro::Lazy<Status> CancelCurrentTask() override;
    void Stop() override;
    Status CleanTask(bool removeTempFiles) override;
    void TEST_SetClock(const std::shared_ptr<util::Clock>& clock);

protected:
    virtual std::string GenerateEpochId() const;

private:
    struct TaskInfo {
        framework::MergeTaskStatus taskStatus;
        std::string taskEpochId;
        int64_t finishedOpCount = -1;
        int64_t totalOpCount = -1;
    };

private:
    TaskInfo GetTaskInfo() const;
    void SetTaskInfo(TaskInfo info);
    void ResetTaskInfo();
    bool NeedSetDesignateTask(const std::map<std::string, std::string>& params) const;

private:
    InitParam _initParam;
    std::unique_ptr<framework::ITabletFactory> _tabletFactory;
    framework::MergeTaskStatus _status;

    mutable std::mutex _infoMutex;
    std::unique_ptr<TaskInfo> _taskInfo;
    std::shared_ptr<util::Clock> _clock = std::make_shared<util::Clock>();

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
