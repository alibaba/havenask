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
#include "indexlib/table/index_task/LocalTabletMergeController.h"

#include "autil/EnvUtil.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/CustomIndexTaskClassInfo.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/framework/TabletFactoryCreator.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/VersionValidator.h"
#include "indexlib/framework/index_task/CustomIndexTaskFactory.h"
#include "indexlib/framework/index_task/CustomIndexTaskFactoryCreator.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IndexTaskContextCreator.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/util/EpochIdUtil.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::table {

AUTIL_LOG_SETUP(indexlib.table, LocalTabletMergeController);

Status LocalTabletMergeController::Init(InitParam param)
{
    assert(param.executor);
    assert(param.schema);
    assert(param.options);
    auto& schema = param.schema;

    auto options = std::make_shared<config::TabletOptions>(*param.options);
    options->SetIsOnline(false);
    param.options = options;

    auto tabletFactoryCreator = framework::TabletFactoryCreator::GetInstance();
    _tabletFactory = tabletFactoryCreator->Create(schema->GetTableType());
    if (!_tabletFactory) {
        AUTIL_LOG(ERROR, "create tablet factory with type [%s] failed, registered types [%s]",
                  schema->GetTableType().c_str(),
                  autil::legacy::ToJsonString(tabletFactoryCreator->GetRegisteredType(), true).c_str());
        return Status::Corruption();
    }
    if (!_tabletFactory->Init(std::make_shared<config::TabletOptions>(*options), nullptr)) {
        AUTIL_LOG(ERROR, "init tablet factory with type [%s] failed", schema->GetTableType().c_str());
        return Status::Corruption();
    }
    _initParam = std::move(param);
    return Status::OK();
}

future_lite::coro::Lazy<std::pair<Status, versionid_t>> LocalTabletMergeController::GetLastMergeTaskResult()
{
    versionid_t mergeResult = INVALID_VERSIONID;
    std::string indexRoot = _initParam.partitionIndexRoot;
    auto schema = _initParam.schema;
    auto indexDir = indexlib::file_system::Directory::GetPhysicalDirectory(indexRoot);

    fslib::FileList fileList;
    std::vector<versionid_t> mergeVersionIdList;
    auto st = framework::VersionLoader::ListVersion(indexDir, &fileList);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "list version in path [%s] failed.", indexRoot.c_str());
        co_return std::make_pair(st, INVALID_VERSIONID);
    }
    for (auto& fileName : fileList) {
        versionid_t versionId = framework::VersionLoader::GetVersionId(fileName);
        if (!(versionId & framework::Version::PUBLIC_VERSION_ID_MASK) &&
            !(versionId & framework::Version::PRIVATE_VERSION_ID_MASK)) {
            mergeVersionIdList.emplace_back(versionId);
        }
    }
    auto latestVersionId = mergeVersionIdList.empty() ? INVALID_VERSIONID : *mergeVersionIdList.rbegin();
    if (latestVersionId != INVALID_VERSIONID) {
        auto status = framework::VersionValidator::Validate(indexRoot, schema, latestVersionId);
        if (status.IsOK()) {
            AUTIL_LOG(INFO, "recover merge result version [%d] from index root [%s].", latestVersionId,
                      indexRoot.c_str());
            mergeResult = latestVersionId;
        }
    }
    co_return std::make_pair(Status::OK(), mergeResult);
}

void LocalTabletMergeController::TEST_SetClock(const std::shared_ptr<util::Clock>& clock) { _clock = clock; }

struct Env {
    std::shared_ptr<framework::IndexTaskContext> ctx;
    std::shared_ptr<framework::IndexTaskPlan> plan;
    std::shared_ptr<framework::LocalExecuteEngine> engine;
};

std::pair<Status, std::shared_ptr<Env>> LocalTabletMergeController::CloneEnv(framework::IndexTaskContext* context)
{
    Status status;
    auto env = std::make_shared<Env>();
    env->ctx = CreateTaskContext(context->GetBaseVersionId(), context->GetTaskType(), context->GetTaskName(),
                                 context->GetTaskTraceId(), context->GetAllParameters());
    if (env->ctx == nullptr) {
        return {Status::InternalError("create task context failed"), nullptr};
    }
    auto planCreator = _tabletFactory->CreateIndexTaskPlanCreator();
    auto designateTaskConfig = context->GetDesignateTaskConfig();
    if (designateTaskConfig) {
        auto [status, customPlanCreator] = framework::CustomIndexTaskFactory::GetCustomPlanCreator(designateTaskConfig);
        if (!status.IsOK()) {
            return {status, nullptr};
        }
        if (customPlanCreator) {
            planCreator = std::move(customPlanCreator);
        }
    }
    if (planCreator == nullptr) {
        return {Status::InternalError("create task plan creator failed"), nullptr};
    }
    std::tie(status, env->plan) = planCreator->CreateTaskPlan(env->ctx.get());
    if (!status.IsOK()) {
        return {status, nullptr};
    }
    if (env->plan == nullptr) {
        assert(false);
        return {Status::InternalError("empty plan, do not merge"), nullptr};
    }
    auto extendResources = context->GetResourceManager()->TEST_GetExtendResources();
    env->ctx->GetResourceManager()->TEST_SetExtendResources(extendResources);
    auto opCreator = _tabletFactory->CreateIndexOperationCreator(env->ctx->GetTabletSchema());
    if (designateTaskConfig) {
        auto [status, customOpCreator] = framework::CustomIndexTaskFactory::GetCustomOperationCreator(
            designateTaskConfig, context->GetTabletSchema());
        if (!status.IsOK()) {
            return {status, nullptr};
        }
        if (customOpCreator) {
            opCreator = std::move(customOpCreator);
        }
    }
    if (opCreator == nullptr) {
        return {Status::Corruption("create index operation creator failed"), nullptr};
    }
    env->engine = std::make_shared<framework::LocalExecuteEngine>(_initParam.executor, std::move(opCreator));
    return {Status::OK(), env};
}

future_lite::coro::Lazy<Status>
LocalTabletMergeController::SubmitMergeTask(std::unique_ptr<framework::IndexTaskPlan> plan,
                                            framework::IndexTaskContext* context)
{
    auto opCreator = _tabletFactory->CreateIndexOperationCreator(context->GetTabletSchema());
    auto designateTaskConfig = context->GetDesignateTaskConfig();
    if (designateTaskConfig) {
        auto [status, customOpCreator] = framework::CustomIndexTaskFactory::GetCustomOperationCreator(
            designateTaskConfig, context->GetTabletSchema());
        if (!status.IsOK()) {
            co_return status;
        }
        if (customOpCreator) {
            opCreator = std::move(customOpCreator);
        }
    }
    if (!opCreator) {
        AUTIL_LOG(ERROR, "create index operation creator failed");
        co_return Status::Corruption();
    }
    framework::LocalExecuteEngine engine(_initParam.executor, std::move(opCreator));
    int64_t totalOpCount = plan->GetOpDescs().size();
    if (plan->GetEndTaskOpDesc()) {
        ++totalOpCount;
    }
    TaskInfo taskInfo;
    // TODO: update finishedOpCount from local engine
    taskInfo.finishedOpCount = 0;
    taskInfo.totalOpCount = totalOpCount;
    auto baseVersion = context->GetTabletData()->GetOnDiskVersion();
    taskInfo.taskStatus.baseVersion = baseVersion;
    taskInfo.taskEpochId = context->GetTaskEpochId();

    SetTaskInfo(taskInfo);
    Status status;
    std::string resultStr;
    framework::MergeTaskStatus taskStatus;
    if (autil::EnvUtil::getEnv("IS_TEST_MODE", false)) {
        size_t parallelNum = autil::EnvUtil::getEnv("TEST_LOCAL_ENGINE_PARALLEL_NUM", 2);
        std::vector<std::shared_ptr<Env>> envs;
        std::vector<future_lite::coro::Lazy<Status>> tasks;
        for (size_t i = 0; i < parallelNum; i++) {
            auto [s, env] = CloneEnv(context);
            if (!s.IsOK()) {
                taskInfo.taskStatus.code = framework::MergeTaskStatus::ERROR;
                SetTaskInfo(taskInfo);
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                co_return s;
            }
            envs.push_back(env);
        }
        for (auto& env : envs) {
            tasks.push_back(env->engine->ScheduleTask(*(env->plan), env->ctx.get()));
        }
        auto rets = co_await future_lite::coro::collectAll(std::move(tasks));
        size_t idx = 0;
        status = rets[idx].value();
        for (size_t i = 0; i < parallelNum; i++) {
            if (rets[i].value().IsOK()) {
                idx = i;
                status = rets[i].value();
            }
        }
        resultStr = envs[idx]->ctx->GetResult();
    } else {
        status = co_await engine.ScheduleTask(*plan, context);
        resultStr = context->GetResult();
    }
    taskStatus.baseVersion = baseVersion;
    if (!status.IsOK()) {
        taskInfo.taskStatus.code = framework::MergeTaskStatus::ERROR;
        SetTaskInfo(taskInfo);
        AUTIL_LOG(ERROR, "schedule task failed: %s", status.ToString().c_str());
        co_return status;
    }
    AUTIL_LOG(INFO, "merge result: %s", resultStr.c_str());
    if (resultStr.empty()) {
        taskInfo.taskStatus.code = framework::MergeTaskStatus::ERROR;
        SetTaskInfo(taskInfo);
        AUTIL_LOG(ERROR, "task result is empty");
        co_return Status::InternalError("task result is empty");
    }
    framework::MergeResult result;
    status = indexlib::file_system::JsonUtil::FromString(resultStr, &result).Status();
    if (!status.IsOK()) {
        taskInfo.taskStatus.code = framework::MergeTaskStatus::ERROR;
        SetTaskInfo(taskInfo);
        co_return status;
    }
    if (result.targetVersionId == INVALID_VERSIONID) {
        taskInfo.taskStatus.code = framework::MergeTaskStatus::ERROR;
        SetTaskInfo(taskInfo);
        AUTIL_LOG(ERROR, "invalid target version id");
        co_return Status::Corruption("invalid target version id.");
    }
    status = framework::VersionLoader::GetVersion(context->GetIndexRoot(), result.targetVersionId,
                                                  &(taskStatus.targetVersion));
    if (!status.IsOK()) {
        taskInfo.taskStatus.code = framework::MergeTaskStatus::ERROR;
        SetTaskInfo(taskInfo);
        co_return status;
    }
    taskStatus.code = framework::MergeTaskStatus::DONE;
    taskInfo.finishedOpCount = totalOpCount;
    taskInfo.taskStatus = taskStatus;

    SetTaskInfo(taskInfo);
    co_return Status::OK();
}

future_lite::coro::Lazy<std::pair<Status, framework::MergeTaskStatus>> LocalTabletMergeController::WaitMergeResult()
{
    if (!GetRunningTaskStat()) {
        co_return std::make_pair(Status::InternalError(), framework::MergeTaskStatus());
    }
    auto result = std::make_pair(Status::OK(), GetTaskInfo().taskStatus);
    co_return result;
}

std::optional<framework::ITabletMergeController::TaskStat> LocalTabletMergeController::GetRunningTaskStat() const
{
    std::lock_guard<std::mutex> lock(_infoMutex);
    if (!_taskInfo) {
        return std::nullopt;
    }
    TaskStat stat;
    stat.baseVersionId = _taskInfo->taskStatus.baseVersion.GetVersionId();
    stat.finishedOpCount = _taskInfo->finishedOpCount;
    stat.totalOpCount = _taskInfo->totalOpCount;
    return stat;
}

void LocalTabletMergeController::Stop() {}

future_lite::coro::Lazy<Status> LocalTabletMergeController::CancelCurrentTask() { co_return Status::OK(); }

Status LocalTabletMergeController::CleanTask(bool removeTempFiles)
{
    if (!GetRunningTaskStat()) {
        return Status::OK();
    }
    auto taskInfo = GetTaskInfo();
    auto ret = Status::OK();
    if (removeTempFiles) {
        auto partitionTempWorkRoot = PathUtil::GetTaskTempWorkRoot(_initParam.partitionIndexRoot, taskInfo.taskEpochId);
        if (partitionTempWorkRoot.empty()) {
            AUTIL_LOG(ERROR, "task [%s] temp work root is empty", taskInfo.taskEpochId.c_str());
            return Status::InternalError();
        }
        ret = indexlib::file_system::FslibWrapper::DeleteDir(partitionTempWorkRoot,
                                                             indexlib::file_system::DeleteOption::MayNonExist())
                  .Status();
        AUTIL_LOG(INFO, "task [%s] cleaned: %s", taskInfo.taskEpochId.c_str(), ret.ToString().c_str());
    }
    ResetTaskInfo();
    return ret;
}

std::string LocalTabletMergeController::GenerateEpochId() const
{
    return indexlib::util::EpochIdUtil::GenerateEpochId(autil::TimeUtility::currentTimeInMicroSeconds());
}

std::unique_ptr<framework::IndexTaskContext>
LocalTabletMergeController::CreateTaskContext(versionid_t baseVersionId, const std::string& taskType,
                                              const std::string& taskName, const std::string& taskTraceId,
                                              const std::map<std::string, std::string>& params)
{
    std::string epochId = GenerateEpochId();
    const std::string sourceVersionRoot =
        _initParam.buildTempIndexRoot.empty() ? _initParam.partitionIndexRoot : _initParam.buildTempIndexRoot;

    framework::IndexTaskContextCreator contextCreator;
    contextCreator.SetTabletName(_initParam.schema->GetTableName())
        .SetTabletSchema(_initParam.schema)
        .SetTabletOptions(_initParam.options)
        .SetTaskEpochId(epochId)
        .SetExecuteEpochId(epochId)
        .SetTabletFactory(_tabletFactory.get())
        .SetMemoryQuotaController(_initParam.memoryQuotaController)
        .SetDestDirectory(_initParam.partitionIndexRoot)
        .SetMetricProvider(_initParam.metricProvider)
        .SetClock(_clock)
        .SetTaskType(taskType)
        .SetTaskName(taskName)
        .AddSourceVersion(sourceVersionRoot, baseVersionId)
        .SetTaskParams(params)
        .AddParameter(BRANCH_ID, std::to_string(_initParam.branchId));
    if (!taskType.empty()) {
        contextCreator.SetDesignateTask(taskType, taskName);
    }
    auto context = contextCreator.CreateContext();
    if (context) {
        auto resourceManager = context->GetResourceManager();
        for (auto extendResource : _initParam.extendResources) {
            auto status = resourceManager->AddExtendResource(extendResource);
            if (!status.IsOK()) {
                return nullptr;
            }
        }
    }
    return context;
}

LocalTabletMergeController::TaskInfo LocalTabletMergeController::GetTaskInfo() const
{
    std::lock_guard<std::mutex> lock(_infoMutex);
    assert(_taskInfo);
    return *_taskInfo;
}

void LocalTabletMergeController::SetTaskInfo(LocalTabletMergeController::TaskInfo info)
{
    std::lock_guard<std::mutex> lock(_infoMutex);
    _taskInfo = std::make_unique<TaskInfo>(std::move(info));
}

void LocalTabletMergeController::ResetTaskInfo()
{
    std::lock_guard<std::mutex> lock(_infoMutex);
    _taskInfo.reset();
}

} // namespace indexlibv2::table
