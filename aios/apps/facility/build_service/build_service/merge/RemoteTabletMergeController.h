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

#include <mutex>
#include <random>

#include "build_service/common/RpcChannelManager.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/framework/ITabletMergeController.h"

namespace indexlibv2::config {
class TabletSchema;
class TabletOptions;
} // namespace indexlibv2::config

namespace indexlibv2::framework {
class ITabletFactory;
} // namespace indexlibv2::framework

namespace build_service::merge {

// TODO(hanyao): derive this from LocalTabletMergeController
class RemoteTabletMergeController : public indexlibv2::framework::ITabletMergeController
{
private:
    using IndexTaskContext = indexlibv2::framework::IndexTaskContext;
    using IndexTaskPlan = indexlibv2::framework::IndexTaskPlan;
    using MergeTaskStatus = indexlibv2::framework::MergeTaskStatus;
    using TaskStat = indexlibv2::framework::ITabletMergeController::TaskStat;

public:
    struct InitParam {
        future_lite::Executor* executor = nullptr;
        std::shared_ptr<indexlibv2::config::TabletSchema> schema;
        std::shared_ptr<indexlibv2::config::TabletOptions> options;
        std::string remotePartitionIndexRoot;
        std::string buildTempIndexRoot;
        int32_t generationId = -1;
        std::string tableName;
        uint16_t rangeFrom = 0;
        uint16_t rangeTo = 0;
        uint64_t branchId = 0;
        std::string configPath;
        std::string dataTable;
        std::string bsServerAddress;
    };

public:
    RemoteTabletMergeController() = default;
    virtual ~RemoteTabletMergeController() = default;

    indexlib::Status Init(InitParam param);

public:
    future_lite::coro::Lazy<indexlib::Status> Recover() override;
    std::optional<TaskStat> GetRunningTaskStat() const override;
    future_lite::coro::Lazy<std::pair<indexlib::Status, indexlibv2::versionid_t>> GetLastMergeTaskResult() override;
    std::unique_ptr<IndexTaskContext> CreateTaskContext(indexlibv2::versionid_t baseVersionId,
                                                        const std::string& taskType, const std::string& taskName,
                                                        const std::map<std::string, std::string>& params) override;
    future_lite::coro::Lazy<indexlib::Status> SubmitMergeTask(std::unique_ptr<IndexTaskPlan> plan,
                                                              IndexTaskContext* context) override;
    future_lite::coro::Lazy<std::pair<indexlib::Status, MergeTaskStatus>> WaitMergeResult() override;
    indexlib::Status CleanTask(bool removeTempFiles) override;
    future_lite::coro::Lazy<indexlib::Status> CancelCurrentTask() override;
    void Stop() override;

protected:
    std::unique_ptr<indexlibv2::framework::ITabletFactory>
    CreateTabletFactory(RemoteTabletMergeController::InitParam& param);
    std::unique_ptr<common::RpcChannelManager> CreateRpcChannelManager(const std::string& serviceAddress);

protected:
    // virtual for test
    virtual future_lite::coro::Lazy<bool> SubmitTask(std::unique_ptr<IndexTaskPlan> plan, IndexTaskContext* context,
                                                     proto::InformResponse* response);
    virtual future_lite::coro::Lazy<bool> GetTaskInfo(int64_t taskId, proto::TaskInfoResponse* response);
    virtual future_lite::coro::Lazy<bool> StopTask(int64_t taskId);
    virtual future_lite::coro::Lazy<bool> StopBuild();
    virtual future_lite::coro::Lazy<bool> GetGenerationInfo(proto::GenerationInfo* generationInfo);

private:
    struct TaskDescription {
        int64_t taskId = -1;
        int32_t taskEpochId = -1;
        indexlibv2::versionid_t baseVersionId = indexlibv2::INVALID_VERSIONID;
        int64_t finishedOpCount = 0;
        int64_t totalOpCount = 0;
    };

private:
    int64_t GenerateTaskId(int32_t timestamp) const;
    int32_t ExtractTaskEpochId(int64_t taskId) const;
    void fillPlan(const IndexTaskPlan& plan, proto::OperationPlan* planMessage) const;
    void SetTaskDescription(int64_t taskId, int32_t taskEpochId, indexlibv2::versionid_t baseVersionId);
    void ResetTaskDescription();
    TaskDescription GetTaskDescription() const;
    void UpdateTaskDescription(int64_t finishedOpCount, int64_t totalOpCount);
    size_t GetBackoffWindow() const;
    future_lite::coro::Lazy<indexlib::Status> DoRecover();
    future_lite::coro::Lazy<std::pair<indexlib::Status, indexlibv2::versionid_t>> DoGetLastMergeTaskResult();
    void FillBuildId(proto::BuildId* buildId) const;
    future_lite::coro::Lazy<std::pair<indexlib::Status, indexlibv2::versionid_t>>
    GetRunningMergeTaskResult(proto::GenerationInfo generationInfo);
    future_lite::coro::Lazy<std::pair<indexlib::Status, indexlibv2::versionid_t>>
    GetFinishedMergeTaskResult(proto::GenerationInfo generationInfo) const;
    std::string GetSourceRoot() const
    {
        return _initParam.buildTempIndexRoot.empty() ? _initParam.remotePartitionIndexRoot
                                                     : _initParam.buildTempIndexRoot;
    }
    bool NeedSetDesignateTask(const std::map<std::string, std::string>& params) const;

private:
    std::mutex _taskMutex;
    uint64_t _branchId = 0;
    int64_t _startTimestamp = -1;
    InitParam _initParam;
    bool _recovered = false;
    std::atomic<bool> _stopFlag {false};
    std::unique_ptr<indexlibv2::framework::ITabletFactory> _tabletFactory;
    std::unique_ptr<common::RpcChannelManager> _rpcChannelManager;

    mutable std::mutex _taskDescMutex;
    std::unique_ptr<TaskDescription> _submittedTaskDescription;

    mutable std::mt19937 _random;
    mutable std::unique_ptr<std::uniform_int_distribution<size_t>> _uniform;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::merge
