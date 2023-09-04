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
#include "build_service/merge/RemoteTabletMergeController.h"

#include "autil/EnvUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/proto/GeneralTaskInfo.h"
#include "build_service/proto/ProtoUtil.h"
#include "fslib/util/FileUtil.h"
#include "future_lite/coro/Sleep.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FenceDirectory.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/TabletFactoryCreator.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskContextCreator.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/util/EpochIdUtil.h"
#include "indexlib/util/PathUtil.h"

using indexlib::Status;
using indexlibv2::framework::IndexTaskContext;
using indexlibv2::framework::IndexTaskPlan;
using indexlibv2::framework::MergeTaskStatus;

namespace {

const size_t RPC_TIMEOUT = autil::EnvUtil::getEnv<size_t>("TABLET_MERGE_REMOTE_RPC_TIMEOUT", 30 * 1000);
const size_t BACKOFF_WINDOW = autil::EnvUtil::getEnv<size_t>("TABLET_MERGE_REMOTE_BACKOFF_WINDOW", 10 * 1000);

} // namespace

namespace build_service::merge {
BS_LOG_SETUP(merge, RemoteTabletMergeController);

Status RemoteTabletMergeController::Init(RemoteTabletMergeController::InitParam param)
{
    assert(param.executor);
    assert(param.schema);
    assert(param.options);
    _branchId = param.branchId;

    _startTimestamp = autil::TimeUtility::currentTimeInSeconds();
    std::random_device rd;
    _random.seed(rd());
    _uniform = std::make_unique<std::uniform_int_distribution<size_t>>(0, BACKOFF_WINDOW);

    _tabletFactory = CreateTabletFactory(param);
    if (!_tabletFactory) {
        BS_LOG(ERROR, "create tablet factory failed");
        return Status::Corruption("create tablet factory failed");
    }
    auto options = std::make_shared<indexlibv2::config::TabletOptions>(*param.options);
    auto status = indexlibv2::framework::TabletSchemaLoader::ResolveSchema(options, param.remotePartitionIndexRoot,
                                                                           param.schema.get());
    RETURN_IF_STATUS_ERROR(status, "resolve schema failed");

    auto serviceAddress = param.bsServerAddress;
    if (serviceAddress.empty()) {
        auto configPath = param.configPath;
        std::unique_ptr<config::ResourceReader> resourceReader = std::make_unique<config::ResourceReader>(configPath);
        resourceReader->init();

        config::BuildServiceConfig buildServiceConfig;
        if (!resourceReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
            BS_LOG(ERROR, "fail to get build service config");
            return Status::Corruption();
        }
        serviceAddress = buildServiceConfig.zkRoot;
    }

    _rpcChannelManager = CreateRpcChannelManager(serviceAddress);
    if (!_rpcChannelManager) {
        BS_LOG(ERROR, "create rpc channel manager failed for [%s]", serviceAddress.c_str());
        return Status::Corruption();
    }

    _initParam = std::move(param);
    BS_LOG(INFO,
           "init remote merge controller success, generation [%d], table [%s], dataTable [%s], range[%d, %d], "
           "branchid [%lu], configPath [%s]",
           _initParam.generationId, _initParam.tableName.c_str(), _initParam.dataTable.c_str(),
           (int32_t)_initParam.rangeFrom, (int32_t)_initParam.rangeTo, _initParam.branchId,
           _initParam.configPath.c_str());
    return Status::OK();
}

std::unique_ptr<indexlibv2::framework::ITabletFactory>
RemoteTabletMergeController::CreateTabletFactory(RemoteTabletMergeController::InitParam& param)
{
    auto tabletFactoryCreator = indexlibv2::framework::TabletFactoryCreator::GetInstance();
    auto factory = tabletFactoryCreator->Create(param.schema->GetTableType());
    if (!factory) {
        BS_LOG(ERROR, "create tablet factory with type [%s] failed, registered types [%s]",
               param.schema->GetTableType().c_str(),
               autil::legacy::ToJsonString(tabletFactoryCreator->GetRegisteredType()).c_str());
        return nullptr;
    }
    auto options = std::make_shared<indexlibv2::config::TabletOptions>(*param.options);
    if (!factory->Init(options, nullptr)) {
        BS_LOG(ERROR, "init tablet factory with type [%s] failed", param.schema->GetTableType().c_str());
        return nullptr;
    }
    return factory;
}

std::unique_ptr<common::RpcChannelManager>
RemoteTabletMergeController::CreateRpcChannelManager(const std::string& serviceAddress)
{
    auto rpcChannelManager = std::make_unique<common::RpcChannelManager>();
    if (!rpcChannelManager->Init(serviceAddress)) {
        BS_LOG(ERROR, "init rpc failed on [%s]", serviceAddress.c_str());
        return nullptr;
    }
    return rpcChannelManager;
}

std::unique_ptr<IndexTaskContext>
RemoteTabletMergeController::CreateTaskContext(indexlibv2::versionid_t baseVersionId, const std::string& taskType,
                                               const std::string& taskName,
                                               const std::map<std::string, std::string>& params)
{
    int32_t currentTime = autil::TimeUtility::currentTimeInSeconds();
    int64_t taskId = GenerateTaskId(currentTime);
    if (taskId < 0) {
        BS_LOG(ERROR, "generate task id failed, timestamp[%d], rangeFrom[%d], rangeTo[%d]", currentTime,
               _initParam.rangeFrom, _initParam.rangeTo);
        return nullptr;
    }
    std::string sourceVersionRoot = GetSourceRoot();
    std::string epochId = std::to_string(currentTime);
    auto ctxCreator = indexlibv2::framework::IndexTaskContextCreator()
                          .SetTabletName(_initParam.schema->GetTableName())
                          .SetTabletSchema(_initParam.schema)
                          .SetTabletOptions(_initParam.options)
                          .SetTaskEpochId(epochId)
                          .SetExecuteEpochId(epochId)
                          .SetTabletFactory(_tabletFactory.get())
                          .SetDestDirectory(_initParam.remotePartitionIndexRoot)
                          .SetTaskParams(params)
                          .AddSourceVersion(sourceVersionRoot, baseVersionId)
                          .AddParameter(/*key=*/"_task_id_", std::to_string(taskId))
                          .AddParameter(indexlibv2::table::BRANCH_ID, std::to_string(_branchId));
    if (!taskType.empty()) {
        ctxCreator.SetDesignateTask(taskType, taskName);
    }
    return ctxCreator.CreateContext();
}

future_lite::coro::Lazy<indexlib::Status>
RemoteTabletMergeController::SubmitMergeTask(std::unique_ptr<IndexTaskPlan> plan, IndexTaskContext* context)
{
    int64_t taskId = 0;
    if (!context->GetParameter("_task_id_", taskId)) {
        BS_LOG(ERROR, "task [%s] get task id failed", context->GetTaskEpochId().c_str());
        co_return Status::InternalError();
    }
    int32_t taskEpochId = autil::StringUtil::numberFromString<int32_t>(context->GetTaskEpochId());
    SetTaskDescription(taskId, taskEpochId, context->GetTabletData()->GetOnDiskVersion().GetVersionId());

    for (;;) {
        if (_stopFlag.load()) {
            BS_LOG(WARN, "break submit task epoch [%d] id [%ld]", taskEpochId, taskId);
            co_return Status::Expired();
        }
        proto::InformResponse response;
        auto copiedPlan = std::make_unique<IndexTaskPlan>(*plan);
        auto r = co_await SubmitTask(std::move(copiedPlan), context, &response);
        if (!r) {
            BS_LOG(WARN, "submit task rpc may fail, task epoch[%s]", context->GetTaskEpochId().c_str());
            co_await future_lite::coro::sleep(std::chrono::milliseconds(GetBackoffWindow()));
            continue;
        }
        if (!response.has_errorcode() || response.errorcode() == proto::ADMIN_ERROR_NONE ||
            response.errorcode() == proto::ADMIN_DUPLICATE) {
            break;
        }
        BS_LOG(ERROR, "submit task failed: %s", response.ShortDebugString().c_str());
        co_return Status::InternalError("submit task failed");
    }

    BS_LOG(INFO, "new merge task, task id[%ld], task epoch[%d]", taskId, taskEpochId);
    co_return Status::OK();
}

future_lite::coro::Lazy<std::pair<indexlib::Status, MergeTaskStatus>> RemoteTabletMergeController::WaitMergeResult()
{
    MergeTaskStatus taskStatus;
    if (!GetRunningTaskStat()) {
        co_return std::make_pair(Status::InternalError("no submitted task"), taskStatus);
    }
    auto taskDesc = GetTaskDescription();
    for (;;) {
        if (_stopFlag.load()) {
            BS_LOG(WARN, "break task epoch [%d] id [%ld]", taskDesc.taskEpochId, taskDesc.taskId);
            co_return std::make_pair(Status::Expired(), MergeTaskStatus());
        }
        proto::TaskInfoResponse taskInfoResponse;
        auto r = co_await GetTaskInfo(taskDesc.taskId, &taskInfoResponse);
        if (r) {
            if (!taskInfoResponse.taskexist()) {
                BS_LOG(ERROR, "task epoch [%d] id [%ld] does not exist", taskDesc.taskEpochId, taskDesc.taskId);
                taskStatus.code = MergeTaskStatus::ERROR;
                break;
            }
            if (taskInfoResponse.has_errorcode() && taskInfoResponse.errorcode() != proto::ADMIN_ERROR_NONE) {
                // TODO: log error message
                BS_LOG(ERROR, "task epoch [%d] id [%ld]", taskDesc.taskEpochId, taskDesc.taskId);
                taskStatus.code = MergeTaskStatus::ERROR;
                break;
            }
            const auto& taskInfo = taskInfoResponse.taskinfo();
            assert(taskInfo.has_taskstep());
            assert(taskInfo.has_taskinfo());
            auto [status, generalTaskInfo] =
                indexlib::file_system::JsonUtil::FromString<proto::GeneralTaskInfo>(taskInfo.taskinfo()).StatusWith();
            if (!status.IsOK()) {
                taskStatus.code = MergeTaskStatus::ERROR;
                BS_LOG(ERROR, "jsonize task info[%s] failed", taskInfo.taskinfo().c_str());
                break;
            }
            UpdateTaskDescription(generalTaskInfo.finishedOpCount, generalTaskInfo.totalOpCount);
            if (taskInfo.taskstep() == "running") {
                // pass
            } else if (taskInfo.taskstep() == "finished") {
                auto [mergeStatus, mergeResult] =
                    indexlib::file_system::JsonUtil::FromString<indexlibv2::framework::MergeResult>(
                        generalTaskInfo.result)
                        .StatusWith();
                if (!mergeStatus.IsOK()) {
                    taskStatus.code = MergeTaskStatus::ERROR;
                    BS_LOG(ERROR, "jsonize merge result[%s] failed", generalTaskInfo.result.c_str());
                } else {
                    taskStatus.code = MergeTaskStatus::DONE;
                    auto sourceDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(GetSourceRoot());
                    auto [status, versionPtr] =
                        indexlibv2::framework::VersionLoader::GetVersion(sourceDir, mergeResult.baseVersionId);
                    if (!status.IsOK()) {
                        taskStatus.code = MergeTaskStatus::ERROR;
                        BS_LOG(ERROR, "load source version failed");
                        break;
                    }

                    taskStatus.baseVersion = *versionPtr;
                    auto indexDir =
                        indexlib::file_system::IDirectory::GetPhysicalDirectory(_initParam.remotePartitionIndexRoot);
                    std::tie(status, versionPtr) =
                        indexlibv2::framework::VersionLoader::GetVersion(indexDir, mergeResult.targetVersionId);
                    if (!status.IsOK()) {
                        taskStatus.code = MergeTaskStatus::ERROR;
                        BS_LOG(ERROR, "load target version failed");
                        break;
                    }
                    taskStatus.targetVersion = *versionPtr;
                    BS_LOG(INFO, "merge task [%d] done, base version[%d], target version[%d]", taskDesc.taskEpochId,
                           mergeResult.baseVersionId, mergeResult.targetVersionId);
                }
                break;
            } else if (taskInfo.taskstep() == "stopped") {
                BS_LOG(ERROR, "merge task [%d] stopped", taskDesc.taskEpochId);
                taskStatus.code = MergeTaskStatus::ERROR;
                break;
            } else {
                assert(false);
            }
        }
        co_await future_lite::coro::sleep(std::chrono::milliseconds(GetBackoffWindow()));
    }
    if (taskStatus.code == MergeTaskStatus::ERROR) {
        co_await StopTask(taskDesc.taskId);
    }
    co_return std::make_pair(Status::OK(), taskStatus);
}

int64_t RemoteTabletMergeController::GenerateTaskId(int32_t taskEpochId) const
{
    uint64_t taskId = 0;
    taskId |= (static_cast<uint64_t>(_startTimestamp) << 32);
    taskId |= static_cast<uint64_t>(taskEpochId);
    return static_cast<int64_t>(taskId);
}

int32_t RemoteTabletMergeController::ExtractTaskEpochId(int64_t taskId) const
{
    assert(taskId > 0);
    uint64_t value = taskId;
    return static_cast<int32_t>(value & 0xffffffff);
}

void RemoteTabletMergeController::Stop() { _stopFlag = true; }

namespace {

class Callback : public ::google::protobuf::Closure
{
public:
    Callback(std::coroutine_handle<> handle) : _handle(handle) {}

    void Run() override
    {
        if (_handle) {
            _handle.resume();
        }
    }

private:
    std::coroutine_handle<> _handle;
};

template <typename Request, typename Response>
struct Awaiter {
    Request* request;
    Response* response;
    std::shared_ptr<::google::protobuf::RpcChannel> rpcChannel;
    std::shared_ptr<arpc::ANetRPCController> controller;
    std::function<void(proto::AdminService_Stub* stub, ::google::protobuf::RpcController* controller,
                       const Request* request, Response* response, ::google::protobuf::Closure* done)>
        method;

    Awaiter() : controller(std::make_shared<arpc::ANetRPCController>()) {}

    constexpr bool await_ready() const { return false; }
    void await_suspend(std::coroutine_handle<> h)
    {
        proto::AdminService_Stub stub(rpcChannel.get(), ::google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
        controller->SetExpireTime(RPC_TIMEOUT);
        auto callback = new Callback(h);
        method(&stub, controller.get(), request, response, callback);
    }
    void await_resume() const {}
};

template <typename Request, typename Response>
std::unique_ptr<Awaiter<Request, Response>> MakeAwaiter(common::RpcChannelManager* rpcChannelManager, Request* request,
                                                        Response* response)
{
    auto rpcChannel = rpcChannelManager->getRpcChannel();
    if (!rpcChannel) {
        return nullptr;
    }
    auto awaiter = std::make_unique<Awaiter<Request, Response>>();
    awaiter->request = request;
    awaiter->response = response;
    awaiter->rpcChannel = rpcChannel;
    return awaiter;
}

} // namespace

void RemoteTabletMergeController::fillPlan(const IndexTaskPlan& plan, proto::OperationPlan* planMessage) const
{
    planMessage->set_taskname(plan.GetTaskName());
    planMessage->set_tasktype(plan.GetTaskType());
    for (const auto& op : plan.GetOpDescs()) {
        auto opMessage = planMessage->add_ops();
        opMessage->set_id(op.GetId());
        opMessage->set_type(op.GetType());
        opMessage->set_memory(op.GetEstimateMemory());
        for (auto dependOpId : op.GetDepends()) {
            opMessage->add_depends(dependOpId);
        }
        for (const auto& [key, val] : op.GetAllParameters()) {
            opMessage->add_paramkeys(key);
            opMessage->add_paramvalues(val);
        }
    }
    if (plan.GetEndTaskOpDesc()) {
        auto& op = *plan.GetEndTaskOpDesc();
        auto opMessage = planMessage->mutable_endtaskop();
        opMessage->set_id(op.GetId());
        opMessage->set_type(op.GetType());
        opMessage->set_memory(op.GetEstimateMemory());
        for (const auto& [key, val] : op.GetAllParameters()) {
            opMessage->add_paramkeys(key);
            opMessage->add_paramvalues(val);
        }
    }
}

future_lite::coro::Lazy<bool> RemoteTabletMergeController::SubmitTask(std::unique_ptr<IndexTaskPlan> plan,
                                                                      IndexTaskContext* context,
                                                                      proto::InformResponse* response)
{
    std::lock_guard<std::mutex> guard(_taskMutex);
    proto::StartGeneralTaskRequest request;
    FillBuildId(request.mutable_buildid());

    int64_t taskId = 0;
    if (!context->GetParameter("_task_id_", taskId)) {
        BS_LOG(ERROR, "get task id failed");
        co_return false;
    }
    if (taskId <= 0) {
        BS_LOG(ERROR, "taskId[%ld] error, task epoch[%s], range[%d, %d]", taskId, context->GetTaskEpochId().c_str(),
               _initParam.rangeFrom, _initParam.rangeTo);
        co_return false;
    }
    request.set_clustername(_initParam.tableName);
    request.set_taskid(taskId);
    request.set_partitionindexroot(_initParam.remotePartitionIndexRoot);
    request.set_taskepochid(context->GetTaskEpochId());
    request.set_sourceversionid(context->GetTabletData()->GetOnDiskVersion().GetVersionId());
    request.set_branchid(_branchId);
    request.mutable_range()->set_from(_initParam.rangeFrom);
    request.mutable_range()->set_to(_initParam.rangeTo);
    fillPlan(*plan, request.mutable_plan());
    const auto& params = context->GetAllParameters();
    for (const auto& [key, val] : params) {
        request.add_paramkeys(key);
        request.add_paramvalues(val);
    }
    std::string sourceRoot = GetSourceRoot();
    if (sourceRoot != _initParam.remotePartitionIndexRoot) {
        request.add_paramkeys(config::BS_GENERAL_TASK_SOURCE_ROOT);
        request.add_paramvalues(sourceRoot);
    }
    auto awaiter = MakeAwaiter(_rpcChannelManager.get(), &request, response);
    if (!awaiter) {
        BS_LOG(ERROR, "make awaiter failed");
        co_return false;
    }
    awaiter->method = &proto::AdminService_Stub::startGeneralTask;
    BS_LOG(INFO, "prepare to submit request");
    co_await *awaiter;
    auto ret = !awaiter->controller->Failed();
    BS_LOG(INFO, "request submit [%s]", awaiter->controller->ErrorText().c_str());
    co_return ret;
}

future_lite::coro::Lazy<bool> RemoteTabletMergeController::GetTaskInfo(int64_t taskId,
                                                                       proto::TaskInfoResponse* response)
{
    proto::TaskInfoRequest request;
    FillBuildId(request.mutable_buildid());
    request.set_taskid(taskId);

    auto awaiter = MakeAwaiter(_rpcChannelManager.get(), &request, response);
    if (!awaiter) {
        BS_LOG(ERROR, "make awaiter failed");
        co_return false;
    }
    awaiter->method = &proto::AdminService_Stub::getTaskInfo;
    co_await *awaiter;
    co_return !awaiter->controller->Failed();
}

future_lite::coro::Lazy<bool> RemoteTabletMergeController::StopTask(int64_t taskId)
{
    std::lock_guard<std::mutex> guard(_taskMutex);
    proto::StopTaskRequest request;
    proto::InformResponse response;
    FillBuildId(request.mutable_buildid());
    request.set_taskid(taskId);

    auto awaiter = MakeAwaiter(_rpcChannelManager.get(), &request, &response);
    if (!awaiter) {
        BS_LOG(ERROR, "make awaiter failed");
        co_return false;
    }
    awaiter->method = &proto::AdminService_Stub::stopTask;
    co_await *awaiter;
    co_return !awaiter->controller->Failed();
}

future_lite::coro::Lazy<bool> RemoteTabletMergeController::StopBuild()
{
    proto::StopBuildRequest request;
    proto::InformResponse response;
    FillBuildId(request.mutable_buildid());
    request.set_configpath("deprecated");

    auto awaiter = MakeAwaiter(_rpcChannelManager.get(), &request, &response);
    if (!awaiter) {
        BS_LOG(ERROR, "make awaiter failed");
        co_return false;
    }
    awaiter->method = &proto::AdminService_Stub::stopBuild;
    co_await *awaiter;
    co_return !awaiter->controller->Failed();
}

future_lite::coro::Lazy<bool> RemoteTabletMergeController::GetGenerationInfo(proto::GenerationInfo* generationInfo)
{
    proto::ServiceInfoRequest request;
    FillBuildId(request.mutable_buildid());
    request.set_fillflowinfo(true);
    proto::ServiceInfoResponse response;

    auto awaiter = MakeAwaiter(_rpcChannelManager.get(), &request, &response);
    if (!awaiter) {
        BS_LOG(ERROR, "make awaiter failed");
        co_return false;
    }
    awaiter->method = &proto::AdminService_Stub::getServiceInfo;
    co_await *awaiter;
    if (awaiter->controller->Failed()) {
        BS_LOG(ERROR, "call getServiceInfo failed");
        co_return false;
    }
    if (response.has_errorcode() && response.errorcode() != proto::ADMIN_ERROR_NONE) {
        BS_LOG(ERROR, "response has error: %s", response.ShortDebugString().c_str());
        co_return false;
    }
    if (response.generationinfos_size() > 1) {
        BS_LOG(ERROR, "matched generation count [%d] fail", response.generationinfos_size());
        co_return false;
    }
    if (response.generationinfos_size() == 0) {
        BS_LOG(INFO, "no task generation found");
        co_return true;
    }
    *generationInfo = response.generationinfos(0);
    co_return true;
}

indexlib::Status RemoteTabletMergeController::CleanTask(bool removeTempFiles)
{
    if (!GetRunningTaskStat()) {
        return Status::OK();
    }
    auto taskDesc = GetTaskDescription();
    auto ret = Status::OK();
    if (removeTempFiles) {
        auto partitionTempWorkRoot = indexlibv2::PathUtil::GetTaskTempWorkRoot(_initParam.remotePartitionIndexRoot,
                                                                               std::to_string(taskDesc.taskEpochId));
        if (partitionTempWorkRoot.empty()) {
            BS_LOG(ERROR, "task [%d] temp work root is empty", taskDesc.taskEpochId);
            return Status::InternalError();
        }
        ret = indexlib::file_system::FslibWrapper::DeleteDir(partitionTempWorkRoot,
                                                             indexlib::file_system::DeleteOption::MayNonExist())
                  .Status();
    }
    ResetTaskDescription();
    return ret;
}

future_lite::coro::Lazy<std::pair<indexlib::Status, indexlibv2::versionid_t>>
RemoteTabletMergeController::GetRunningMergeTaskResult(proto::GenerationInfo generationInfo)
{
    for (const auto& taskInfo : generationInfo.activetaskinfos()) {
        if (!taskInfo.has_taskname() || taskInfo.taskname() != config::BS_GENERAL_TASK) {
            continue;
        }
        if (!taskInfo.has_taskinfo() || taskInfo.taskinfo().empty()) {
            BS_LOG(ERROR, "task [%s] task info empty", taskInfo.ShortDebugString().c_str());
            continue;
        }
        if (!taskInfo.has_taskidentifier()) {
            BS_LOG(ERROR, "task [%s] has no identifier", taskInfo.ShortDebugString().c_str());
            continue;
        }
        int64_t taskId = -1;
        if (!autil::StringUtil::numberFromString(taskInfo.taskidentifier(), taskId)) {
            BS_LOG(ERROR, "task [%s] identifier invalid", taskInfo.ShortDebugString().c_str());
            continue;
        }
        int32_t taskEpochId = ExtractTaskEpochId(taskId);
        auto [st, generalTaskInfo] =
            indexlib::file_system::JsonUtil::FromString<proto::GeneralTaskInfo>(taskInfo.taskinfo()).StatusWith();
        if (!st.IsOK()) {
            BS_LOG(ERROR, "task [%s] task info error: %s", taskInfo.taskinfo().c_str(), st.ToString().c_str());
            continue;
        }
        if (generalTaskInfo.clusterName != _initParam.tableName) {
            continue;
        }
        SetTaskDescription(taskId, taskEpochId, generalTaskInfo.baseVersionId);
        UpdateTaskDescription(generalTaskInfo.finishedOpCount, generalTaskInfo.totalOpCount);
        auto [status, mergeTaskStatus] = co_await WaitMergeResult();
        if (!status.IsOK()) {
            BS_LOG(ERROR, "wait merge result failed: %s", status.ToString().c_str());
            co_return std::make_pair(Status::InternalError(), indexlibv2::INVALID_VERSIONID);
        }
        auto mergeResult = mergeTaskStatus.targetVersion.GetVersionId();
        co_return std::make_pair(Status::OK(), mergeResult);
    }
    co_return std::make_pair(Status::OK(), indexlibv2::INVALID_VERSIONID);
}

future_lite::coro::Lazy<std::pair<indexlib::Status, indexlibv2::versionid_t>>
RemoteTabletMergeController::GetFinishedMergeTaskResult(proto::GenerationInfo generationInfo) const
{
    int32_t maxTaskEpochId = -1;
    auto finishedMergeResult = indexlibv2::INVALID_VERSIONID;
    for (const auto& taskInfo : generationInfo.stoppedtaskinfos()) {
        if (!taskInfo.has_taskname() || taskInfo.taskname() != config::BS_GENERAL_TASK) {
            continue;
        }
        if (!taskInfo.has_taskstep() || taskInfo.taskstep() != "finished") {
            continue;
        }
        if (!taskInfo.has_taskidentifier()) {
            BS_LOG(ERROR, "task [%s] has no identifier", taskInfo.ShortDebugString().c_str());
            continue;
        }
        int64_t taskId = -1;
        if (!autil::StringUtil::numberFromString(taskInfo.taskidentifier(), taskId)) {
            BS_LOG(ERROR, "task [%s] identifier invalid", taskInfo.ShortDebugString().c_str());
            continue;
        }
        int32_t taskEpochId = ExtractTaskEpochId(taskId);
        if (taskEpochId > maxTaskEpochId) {
            if (!taskInfo.has_taskinfo() || taskInfo.taskinfo().empty()) {
                BS_LOG(ERROR, "task [%s] task info empty", taskInfo.ShortDebugString().c_str());
                continue;
            }
            auto [status, generalTaskInfo] =
                indexlib::file_system::JsonUtil::FromString<proto::GeneralTaskInfo>(taskInfo.taskinfo()).StatusWith();
            if (!status.IsOK()) {
                BS_LOG(ERROR, "task [%s] task info error: %s", taskInfo.taskinfo().c_str(), status.ToString().c_str());
                continue;
            }
            if (generalTaskInfo.clusterName != _initParam.tableName) {
                continue;
            }
            auto [mergeStatus, mergeResult] =
                indexlib::file_system::JsonUtil::FromString<indexlibv2::framework::MergeResult>(generalTaskInfo.result)
                    .StatusWith();
            if (!mergeStatus.IsOK()) {
                BS_LOG(ERROR, "jsonize merge result[%s] failed", generalTaskInfo.result.c_str());
                continue;
            }
            finishedMergeResult = mergeResult.targetVersionId;
            BS_LOG(INFO, "merge task [%d] has already done, target version[%d]", taskEpochId, finishedMergeResult);
            maxTaskEpochId = taskEpochId;
        }
    }
    co_return std::make_pair(Status::OK(), finishedMergeResult);
}

future_lite::coro::Lazy<std::pair<indexlib::Status, indexlibv2::versionid_t>>
RemoteTabletMergeController::GetLastMergeTaskResult()
{
    for (;;) {
        auto [status, versionId] = co_await DoGetLastMergeTaskResult();
        if (status.IsOK()) {
            co_return std::make_pair(status, versionId);
        }
        co_await future_lite::coro::sleep(std::chrono::milliseconds(GetBackoffWindow()));
    }
}

future_lite::coro::Lazy<std::pair<indexlib::Status, indexlibv2::versionid_t>>
RemoteTabletMergeController::DoGetLastMergeTaskResult()
{
    proto::GenerationInfo generationInfo;
    auto ret = co_await GetGenerationInfo(&generationInfo);
    if (!ret) {
        BS_LOG(ERROR, "get generation info failed.");
        co_return std::make_pair(Status::InternalError(), indexlibv2::INVALID_VERSIONID);
    }
    if (generationInfo.has_hasfatalerror() && generationInfo.hasfatalerror()) {
        std::string errorMsg;
        if (generationInfo.has_generationfatalerrormsg()) {
            errorMsg = generationInfo.generationfatalerrormsg();
        }
        BS_LOG(ERROR, "generation has fatal error: %s", errorMsg.c_str());
        co_return std::make_pair(Status::InternalError(), indexlibv2::INVALID_VERSIONID);
    }
    auto [st, runningMergeResult] = co_await GetRunningMergeTaskResult(generationInfo);
    if (st.IsOK() && runningMergeResult != indexlibv2::INVALID_VERSIONID) {
        co_return std::make_pair(st, runningMergeResult);
    }
    co_return co_await GetFinishedMergeTaskResult(generationInfo);
}

future_lite::coro::Lazy<Status> RemoteTabletMergeController::Recover()
{
    if (!_recovered) {
        for (;;) {
            if (_stopFlag.load()) {
                co_return Status::Expired();
            }
            auto status = co_await DoRecover();
            if (status.IsOK()) {
                co_return status;
            }
            co_await future_lite::coro::sleep(std::chrono::milliseconds(GetBackoffWindow()));
        }
    }
    _recovered = true;
    co_return Status::OK();
}

future_lite::coro::Lazy<Status> RemoteTabletMergeController::DoRecover()
{
    proto::GenerationInfo generationInfo;
    auto r = co_await GetGenerationInfo(&generationInfo);
    if (!r) {
        BS_LOG(ERROR, "recover failed");
        co_return Status::InternalError("recover failed");
    }
    if (generationInfo.has_hasfatalerror() && generationInfo.hasfatalerror()) {
        std::string errorMsg;
        if (generationInfo.has_generationfatalerrormsg()) {
            errorMsg = generationInfo.generationfatalerrormsg();
        }
        BS_LOG(ERROR, "generation has fatal error: %s", errorMsg.c_str());
        co_return Status::InternalError("recover failed");
    }
    // TODO(hanyao): recover from iff MERGE task
    for (const auto& taskInfo : generationInfo.activetaskinfos()) {
        if (!taskInfo.has_taskname() || taskInfo.taskname() != config::BS_GENERAL_TASK) {
            continue;
        }
        if (!taskInfo.has_taskidentifier()) {
            BS_LOG(ERROR, "task [%s] has no identifier", taskInfo.ShortDebugString().c_str());
            co_return Status::InternalError("recover failed");
        }
        int64_t taskId = -1;
        if (!autil::StringUtil::numberFromString(taskInfo.taskidentifier(), taskId)) {
            BS_LOG(ERROR, "task [%s] identifier invalid", taskInfo.ShortDebugString().c_str());
            co_return Status::InternalError("recover failed");
        }
        int32_t taskEpochId = ExtractTaskEpochId(taskId);
        if (!taskInfo.has_taskinfo() || taskInfo.taskinfo().empty()) {
            BS_LOG(ERROR, "task [%s] task info empty", taskInfo.ShortDebugString().c_str());
            co_return Status::InternalError("recover failed");
        }
        auto [status, generalTaskInfo] =
            indexlib::file_system::JsonUtil::FromString<proto::GeneralTaskInfo>(taskInfo.taskinfo()).StatusWith();
        if (!status.IsOK()) {
            BS_LOG(ERROR, "task [%s] task info error: %s", taskInfo.taskinfo().c_str(), status.ToString().c_str());
            co_return Status::InternalError("recover failed");
        }
        if (generalTaskInfo.clusterName != _initParam.tableName) {
            continue;
        }
        if (generalTaskInfo.branchId != _branchId) {
            BS_LOG(INFO, "stop task id [%ld] begin, because branch changed from [%lu] to [%lu]", taskId,
                   generalTaskInfo.branchId, _branchId);
            if (!co_await StopTask(taskId)) {
                BS_LOG(ERROR, "stop task [%ld] failed", taskId);
                co_return Status::InternalError("recover failed");
            }
            BS_LOG(INFO, "end recover, drop last task");
            break;
        }
        SetTaskDescription(taskId, taskEpochId, generalTaskInfo.baseVersionId);
        UpdateTaskDescription(generalTaskInfo.finishedOpCount, generalTaskInfo.totalOpCount);
        BS_LOG(INFO, "end recover, running task id[%ld], epoch[%d], baseVersionId[%d]", taskId, taskEpochId,
               generalTaskInfo.baseVersionId);
        break;
    }
    BS_LOG(INFO, "end recover[%s/%d/%hu/%hu]", _initParam.tableName.c_str(), _initParam.generationId,
           _initParam.rangeFrom, _initParam.rangeTo);
    co_return Status::OK();
}

std::optional<RemoteTabletMergeController::TaskStat> RemoteTabletMergeController::GetRunningTaskStat() const
{
    std::lock_guard<std::mutex> lock(_taskDescMutex);
    if (!_submittedTaskDescription) {
        return std::nullopt;
    }
    TaskStat stat;
    stat.baseVersionId = _submittedTaskDescription->baseVersionId;
    stat.finishedOpCount = _submittedTaskDescription->finishedOpCount;
    stat.totalOpCount = _submittedTaskDescription->totalOpCount;
    return stat;
}

void RemoteTabletMergeController::SetTaskDescription(int64_t taskId, int32_t taskEpochId,
                                                     indexlibv2::versionid_t baseVersionId)
{
    std::lock_guard<std::mutex> lock(_taskDescMutex);
    _submittedTaskDescription = std::make_unique<TaskDescription>();
    _submittedTaskDescription->taskId = taskId;
    _submittedTaskDescription->taskEpochId = taskEpochId;
    _submittedTaskDescription->baseVersionId = baseVersionId;
}

void RemoteTabletMergeController::ResetTaskDescription()
{
    std::lock_guard<std::mutex> lock(_taskDescMutex);
    _submittedTaskDescription.reset();
}

RemoteTabletMergeController::TaskDescription RemoteTabletMergeController::GetTaskDescription() const
{
    std::lock_guard<std::mutex> lock(_taskDescMutex);
    assert(_submittedTaskDescription);
    return *_submittedTaskDescription;
}

void RemoteTabletMergeController::UpdateTaskDescription(int64_t finishedOpCount, int64_t totalOpCount)
{
    std::lock_guard<std::mutex> lock(_taskDescMutex);
    _submittedTaskDescription->finishedOpCount = finishedOpCount;
    _submittedTaskDescription->totalOpCount = totalOpCount;
}

size_t RemoteTabletMergeController::GetBackoffWindow() const { return (*_uniform)(_random); }

void RemoteTabletMergeController::FillBuildId(proto::BuildId* buildId) const
{
    if (!_initParam.dataTable.empty()) {
        buildId->set_datatable(_initParam.dataTable);
    }
    buildId->set_generationid(_initParam.generationId);
    std::string appName = proto::ProtoUtil::getGeneralTaskAppName(_initParam.appName, _initParam.tableName,
                                                                  _initParam.rangeFrom, _initParam.rangeTo);
    buildId->set_appname(appName);
}

future_lite::coro::Lazy<Status> RemoteTabletMergeController::CancelCurrentTask()
{
    auto taskDesc = GetTaskDescription();
    if (taskDesc.taskId != -1) {
        BS_LOG(INFO, "begin stop task [%s/%d/%hu/%hu] task id [%ld] ", _initParam.tableName.c_str(),
               _initParam.generationId, _initParam.rangeFrom, _initParam.rangeTo, taskDesc.taskId);
        if (!co_await StopTask(taskDesc.taskId)) {
            BS_LOG(ERROR, "stop task [%s/%d/%hu/%hu] task id [%ld] failed", _initParam.tableName.c_str(),
                   _initParam.generationId, _initParam.rangeFrom, _initParam.rangeTo, taskDesc.taskId);
            co_return Status::InternalError("stop task failed");
        }
        BS_LOG(INFO, "stop task [%s/%d/%hu/%hu] task id [%ld] success", _initParam.tableName.c_str(),
               _initParam.generationId, _initParam.rangeFrom, _initParam.rangeTo, taskDesc.taskId);
    }
    co_return Status::OK();
}

} // namespace build_service::merge
