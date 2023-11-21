#include "indexlib/table/test/TableTestHelper.h"

#include <iomanip>

#include "fslib/util/FileUtil.h"
#include "future_lite/ExecutorCreator.h"
#include "future_lite/NamedTaskScheduler.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/BackgroundTaskConfig.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/CommitOptions.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/TabletDeployer.h"
#include "indexlib/framework/test/TabletTestAgent.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/LocalTabletMergeController.h"
#include "indexlib/table/test/LocalTabletMergeControllerForTest.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, TableTestHelper);

struct TableTestHelper::Impl {
    std::unique_ptr<future_lite::Executor> executor;
    std::unique_ptr<future_lite::TaskScheduler> taskScheduler;
    framework::TabletResource tabletResource;
    std::shared_ptr<framework::Tablet> tablet;
};

TableTestHelper::TableTestHelper(bool autoCleanIndex, bool needDeploy)
    : _autoCleanIndex(autoCleanIndex)
    , _needDeploy(needDeploy)
    , _impl(std::make_unique<TableTestHelper::Impl>())
{
}

TableTestHelper::~TableTestHelper() { Close(); }

std::shared_ptr<framework::ITablet> TableTestHelper::GetITablet() const { return _impl->tablet; }
const std::shared_ptr<framework::Tablet>& TableTestHelper::GetTablet() const { return _impl->tablet; }
std::shared_ptr<indexlibv2::config::ITabletSchema> TableTestHelper::GetSchema() const
{
    assert(_impl->tablet);
    return _impl->tablet->GetTabletSchema();
}

std::shared_ptr<framework::ITabletReader> TableTestHelper::GetTabletReader() const
{
    assert(_impl->tablet);
    return _impl->tablet->GetTabletReader();
}

Status TableTestHelper::TriggerBulkloadTask(framework::MergeTaskStatus& taskStatus)
{
    auto tablet = GetTablet();
    auto tabletSchema = tablet->GetTabletSchema();
    auto tabletOptions = tablet->GetTabletOptions();
    auto tabletInfos = tablet->GetTabletInfos();

    auto factory = framework::TabletFactoryCreator::GetInstance()->Create(tabletSchema->GetTableType());
    assert(factory);
    factory->Init(tabletOptions, nullptr);

    future_lite::executors::SimpleExecutor executor(2);
    LocalTabletMergeController mergeController;
    LocalTabletMergeController::InitParam initParam;
    initParam.executor = &executor;
    initParam.schema = GetSchema();
    initParam.options = tabletOptions;
    initParam.partitionIndexRoot = tabletInfos->GetIndexRoot().GetRemoteRoot();
    auto status = mergeController.Init(initParam);
    RETURN_IF_STATUS_ERROR(status, "mergeController init failed");

    std::string taskType = framework::BULKLOAD_TASK_TYPE;
    std::string taskName = "";
    std::string taskTraceId = "";
    std::map<std::string, std::string> params;
    auto currentVersionId = GetCurrentVersion().GetVersionId();
    auto context = mergeController.CreateTaskContext(currentVersionId, taskType, taskName, taskTraceId, params);
    assert(context);
    auto planCreator = factory->CreateIndexTaskPlanCreator();
    auto [planStatus, plan] = planCreator->CreateTaskPlan(context.get());
    RETURN_IF_STATUS_ERROR(planStatus, "CreateTaskPlan failed");
    assert(plan);

    auto task = [&](auto plan) -> future_lite::coro::Lazy<std::pair<Status, framework::MergeTaskStatus>> {
        auto status = co_await mergeController.SubmitMergeTask(std::move(plan), context.get());
        co_return co_await mergeController.WaitMergeResult();
    };

    std::tie(status, taskStatus) = future_lite::coro::syncAwait(task(std::move(plan)));
    return status;
}

Status TableTestHelper::Bulkload() { return ExecuteTask(framework::BULKLOAD_TASK_TYPE, "bulkload"); }

Status TableTestHelper::ExecuteTask(const std::string& taskType, const std::string& taskName)
{
    Status status;
    framework::Version sourceVersion(INVALID_VERSIONID);
    std::map<std::string, std::string> params;
    versionid_t versionId;
    std::tie(status, versionId) = GetTablet()->ExecuteTask(sourceVersion, taskType, taskName, params);

    if (!GetTablet()->NeedCommit()) {
        return Status::Unimplement("need not commit");
    }
    framework::VersionMeta versionMeta;
    std::tie(status, versionMeta) = GetTablet()->Commit(framework::CommitOptions().SetNeedPublish(true));
    if (!status.IsOK()) {
        return status;
    }
    status = Reopen(framework::ReopenOptions(), versionMeta.GetVersionId());
    if (!status.IsOK()) {
        return status;
    }
    return status;
}

Status TableTestHelper::ImportExternalFiles(const std::string& bulkloadId,
                                            const std::vector<std::string>& externalFiles,
                                            const std::shared_ptr<framework::ImportExternalFileOptions>& options,
                                            framework::Action action, bool commitAndReopen)
{
    auto status = _impl->tablet->ImportExternalFiles(bulkloadId, externalFiles, options, action,
                                                     /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
    if (commitAndReopen) {
        if (_impl->tablet->NeedCommit()) {
            auto [status, version] = Commit();
            RETURN_IF_STATUS_ERROR(status, "commit failed");
            framework::VersionCoord versionCoord(version);
            RETURN_IF_STATUS_ERROR(Reopen(versionCoord), "reopen version failed");
        } else {
            AUTIL_LOG(WARN, "do not need commit, will not commit and reopen for bulkload %s", bulkloadId.c_str());
        }
    }
    return status;
}

Status TableTestHelper::Import(std::vector<framework::Version> versions, framework::ImportOptions importOptions)
{
    auto status = _impl->tablet->Import(versions, importOptions);
    RETURN_IF_STATUS_ERROR(status, "import version list failed");
    if (_impl->tablet->NeedCommit()) {
        auto [status, version] = Commit();
        RETURN_IF_STATUS_ERROR(status, "commit failed");
        framework::VersionCoord versionCoord(version);
        RETURN_IF_STATUS_ERROR(Reopen(versionCoord), "reopen version failed");
    }
    return Status::OK();
}

framework::TabletResource* TableTestHelper::GetTabletResource() { return &(_impl->tabletResource); }

std::shared_ptr<indexlib::file_system::Directory> TableTestHelper::GetRootDirectory() const
{
    return framework::TabletTestAgent(GetTablet()).TEST_GetRootDirectory();
}

Status TableTestHelper::InitTypedTabletResource() { return Status::OK(); }

Status TableTestHelper::InitTabletResource()
{
    _impl->tabletResource.tabletId = indexlib::framework::TabletId("TestHelper");
    if (!_impl->tabletResource.dumpExecutor) {
        _impl->executor = future_lite::ExecutorCreator::Create(
            /*type=*/"simple", future_lite::ExecutorCreator::Parameters().Set<size_t>("threadNum", 5));
        _impl->tabletResource.dumpExecutor = _impl->executor.get();
    }
    if (!_impl->tabletResource.taskScheduler) {
        _impl->taskScheduler = std::make_unique<future_lite::TaskScheduler>(_impl->executor.get());
        _impl->tabletResource.taskScheduler = _impl->taskScheduler.get();
    }
    if (!_impl->tabletResource.mergeController) {
        auto mergeController = std::make_shared<LocalTabletMergeControllerForTest>();
        LocalTabletMergeController::InitParam initParam;
        initParam.executor = _impl->executor.get();
        initParam.schema = _schema;
        initParam.options = _options;
        initParam.partitionIndexRoot = _indexRoot.GetRemoteRoot();
        if (auto st = mergeController->Init(initParam); !st.IsOK()) {
            return st;
        }
        _impl->tabletResource.mergeController = mergeController;
    }
    if (!_impl->tabletResource.metricsReporter) {
        _impl->tabletResource.metricsReporter =
            std::make_shared<kmonitor::MetricsReporter>("TestHelper", "TestHelper", kmonitor::MetricsTags());
    }
    RETURN_STATUS_DIRECTLY_IF_ERROR(InitTypedTabletResource());

    return Status::OK();
}

Status TableTestHelper::Open(const framework::IndexRoot& indexRoot, std::shared_ptr<config::ITabletSchema> tabletSchema,
                             std::shared_ptr<config::TabletOptions> tabletOptions, framework::VersionCoord versionCoord)
{
    _schema = tabletSchema;
    _options = tabletOptions;
    _indexRoot = indexRoot;
    if (_indexRoot.GetLocalRoot() == _indexRoot.GetRemoteRoot()) {
        _autoCleanIndex = false;
        _needDeploy = false;
    }
    if (auto st = InitTabletResource(); !st.IsOK()) {
        return st;
    }
    _impl->tablet = std::make_shared<framework::Tablet>(_impl->tabletResource);
    auto st = GetTablet()->Open(indexRoot, _schema, _options, versionCoord);
    framework::TabletTestAgent(GetTablet()).TEST_DeleteTask(/*taskName=*/"MERGE_VERSION");
    if (st.IsOK()) {
        AfterOpen();
    }
    return st;
}

Status TableTestHelper::Reopen(const framework::VersionCoord& versionCoord)
{
    return Reopen(framework::ReopenOptions(), versionCoord);
}

Status TableTestHelper::Reopen(const framework::ReopenOptions& reopenOptions,
                               const framework::VersionCoord& versionCoord)
{
    auto tabletInfos = GetTablet()->GetTabletInfos();
    if (_needDeploy && _options->IsOnline() &&
        (versionCoord.GetVersionId() & framework::Version::PUBLIC_VERSION_ID_MASK)) {
        if (!Deploy(tabletInfos->GetIndexRoot(), _options.get(), tabletInfos->GetLoadedPublishVersion().GetVersionId(),
                    versionCoord.GetVersionId())) {
            AUTIL_LOG(ERROR, "deploy failed");
            return Status::InternalError("deploy failed");
        }
    }
    auto status = GetTablet()->Reopen(reopenOptions, versionCoord);
    RETURN_IF_STATUS_ERROR(status, "reopen version[%s] failed", versionCoord.DebugString().c_str());
    if (_autoCleanIndex) {
        status = GetTablet()->CleanIndexFiles({});
    }
    return status;
}

const framework::Version& TableTestHelper::GetCurrentVersion() const
{
    return framework::TabletTestAgent(GetTablet()).TEST_GetTabletData()->GetOnDiskVersion();
}

future_lite::NamedTaskScheduler* TableTestHelper::GetTaskScheduler() const
{
    return framework::TabletTestAgent(GetTablet()).TEST_GetTaskScheduler();
}

Status TableTestHelper::Build(std::string docs, bool oneBatch) { return DoBuild(docs, oneBatch); }
Status TableTestHelper::Build(const std::string& docStr, const framework::Locator& locator)
{
    return DoBuild(docStr, locator);
}

Status TableTestHelper::BuildSegment(std::string docs, bool oneBatch, bool autoDumpAndReload)
{
    RETURN_IF_STATUS_ERROR(Build(docs, oneBatch), "build doc failed");
    if (!autoDumpAndReload) {
        framework::TabletTestAgent(GetTablet()).TEST_SealSegment();
        return Status::OK();
    }
    RETURN_IF_STATUS_ERROR(GetTablet()->Flush(), "flush segment failed");
    if (!GetTablet()->NeedCommit()) {
        return Status::OK();
    }
    bool needReopen = GetTablet()->GetTabletOptions()->FlushLocal();
    auto [status, versionMeta] =
        GetTablet()->Commit(framework::CommitOptions().SetNeedPublish(true).SetNeedReopenInCommit(needReopen));
    RETURN_IF_STATUS_ERROR(status, "commit version failed");
    if (!needReopen) {
        auto versionId = versionMeta.GetVersionId();
        return Reopen(framework::ReopenOptions(), versionId);
    }
    return Status::OK();
}

Status TableTestHelper::Flush() { return GetTablet()->Flush(); }
Status TableTestHelper::Seal() { return GetTablet()->Seal(); }
Status TableTestHelper::TriggerDump() { return framework::TabletTestAgent(GetTablet()).TEST_TriggerDump(); }
bool TableTestHelper::NeedCommit() const { return GetTablet()->NeedCommit(); }
std::pair<Status, versionid_t> TableTestHelper::Commit(const framework::CommitOptions& commitOptions)
{
    auto [st, versionMeta] = GetTablet()->Commit(commitOptions);
    return {st, versionMeta.GetVersionId()};
}

std::pair<Status, versionid_t> TableTestHelper::Commit()
{
    return Commit(framework::CommitOptions().SetNeedPublish(true));
}

Status TableTestHelper::AlterTable(std::shared_ptr<config::ITabletSchema> tabletSchema,
                                   const std::vector<std::shared_ptr<framework::IndexTaskResource>>& extendResources,
                                   bool autoCommit, StepInfo* step)
{
    std::unique_ptr<StepInfo> holder;
    Status status;
    StepInfo* stepInfo = step;
    if (!stepInfo) {
        holder = std::make_unique<StepInfo>();
        stepInfo = holder.get();
    }

    while (true) {
        if (stepInfo->stepMax != std::nullopt && stepInfo->stepNum >= stepInfo->stepMax.value()) {
            break;
        }
        std::tie(status, *stepInfo) = StepAlterTable(*stepInfo, tabletSchema, extendResources);
        RETURN_IF_STATUS_ERROR(status, "");
        if (!autoCommit && stepInfo->stepNum == 4) {
            return Status::OK();
        }
        if (stepInfo->isFinished) {
            return Status::OK();
        }
    }
    return Status::OK();
}

Status TableTestHelper::ExecuteMerge(const MergeOption& option)
{
    bool isOptimizeMerge = option.isOptimizeMerge;
    std::string taskName = option.taskName;
    framework::Version sourceVersion(INVALID_VERSIONID);
    std::string taskType;
    std::map<std::string, std::string> params;
    if (isOptimizeMerge) {
        params["is_optimize_merge"] = "true";
        params["optimize_merge"] = "true";
    }
    if (!taskName.empty()) {
        taskType = MERGE_TASK_TYPE;
    }
    if (!option.mergeStrategy.empty()) {
        config::MergeConfig mergeConfig;
        mergeConfig.TEST_SetMergeStrategyStr(option.mergeStrategy);
        mergeConfig.TEST_SetMergeStrategyParameterStr(option.mergeStrategyParam);
        taskType = MERGE_TASK_TYPE;
        config::IndexTaskConfig taskConfig(taskName, taskType, "manual");
        taskConfig.FillMergeConfig(taskName, mergeConfig);
        params[framework::INDEX_TASK_CONFIG_IN_PARAM] = autil::legacy::ToJsonString(taskConfig);
    }
    auto [status, verisonId] = GetTablet()->ExecuteTask(sourceVersion, taskType, taskName, params);
    return status;
}

std::pair<Status, TableTestHelper::StepInfo>
TableTestHelper::StepAlterTable(StepInfo step, std::shared_ptr<config::ITabletSchema> tabletSchema,
                                std::vector<std::shared_ptr<framework::IndexTaskResource>> extendResources)
{
    StepInfo result = step;
    auto mergeController =
        (LocalTabletMergeControllerForTest*)framework::TabletTestAgent(GetTablet()).TEST_GetMergeController().get();
    mergeController->SetTaskPlan(step.taskPlan);
    mergeController->TEST_SetSpecifyEpochId(step.specifyEpochId);
    AddAlterTabletResourceIfNeed(extendResources, tabletSchema);
    mergeController->SetTaskResources(extendResources);
    if (step.stepNum < CHANGE_SCHEMA) {
        auto status = GetTablet()->AlterTable(tabletSchema);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "Alter Table failed: %s", status.ToString().c_str());
            return std::make_pair(status, result);
        }
        _schema = tabletSchema;
        result.stepNum = CHANGE_SCHEMA;
        return std::make_pair(status, result);
    }
    if (step.stepNum < COMMIT_SCHEMA_VERSION) {
        auto status = TriggerDump();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "dump failed: %s", status.ToString().c_str());
            return std::make_pair(status, result);
        }
        if (!GetTablet()->NeedCommit()) {
            AUTIL_LOG(WARN, "alter table step 2 not commit, may be commit by others");
            result.msg = "SKIP_COMMIT_SCHEMA";
            result.stepNum = REOPEN_SCHEMA_VERSION;
            return std::make_pair(Status::OK(), result);
        }
        auto [commitStatus, versionMeta] = GetTablet()->Commit(framework::CommitOptions().SetNeedPublish(true));
        if (!commitStatus.IsOK()) {
            AUTIL_LOG(ERROR, "commit fail: %s", commitStatus.ToString().c_str());
            return std::make_pair(commitStatus, result);
        }
        result.stepResultVersion = versionMeta.GetVersionId();
        result.stepNum = COMMIT_SCHEMA_VERSION;
        return std::make_pair(commitStatus, result);
    }

    if (step.stepNum < REOPEN_SCHEMA_VERSION) {
        auto status = Reopen(framework::ReopenOptions(), step.stepResultVersion);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "reopen fail: %s", status.ToString().c_str());
            return std::make_pair(status, result);
        }
        result.stepNum = REOPEN_SCHEMA_VERSION;
        return std::make_pair(status, result);
    }

    if (step.stepNum < EXECUTE_ALTER_TABLE) {
        MergeOption mergeOption;
        auto status = ExecuteMerge(mergeOption);
        result.taskPlan = mergeController->GetTaskPlan();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "Execute inner task failed.");
            return std::make_pair(status, result);
        }
        if (!GetTablet()->NeedCommit()) {
            AUTIL_LOG(ERROR, "no commit after merge");
            return std::make_pair(Status::InternalError("not commit after merge"), result);
        }
        result.stepNum = EXECUTE_ALTER_TABLE;
        return std::make_pair(Status::OK(), result);
    }

    if (step.stepNum < COMMIT_RESULT) {
        if (!GetTablet()->NeedCommit()) {
            AUTIL_LOG(ERROR, "no commit after merge");
            return std::make_pair(Status::InternalError("not commit after merge"), result);
        }
        auto [commitStatus, versionMeta] = GetTablet()->Commit(framework::CommitOptions().SetNeedPublish(true));
        if (!commitStatus.IsOK()) {
            AUTIL_LOG(ERROR, " commit failed: %s", commitStatus.ToString().c_str());
            return std::make_pair(commitStatus, result);
        }
        result.stepResultVersion = versionMeta.GetVersionId();
        result.stepNum = COMMIT_RESULT;
        return std::make_pair(Status::OK(), result);
    }

    if (step.stepNum < REOPEN_RESULT) {
        auto status = Reopen(framework::ReopenOptions(), step.stepResultVersion);
        if (!status.IsOK()) {
            return std::make_pair(status, result);
        }
        result.stepNum = REOPEN_RESULT;
        result.isFinished = true;
        return std::make_pair(status, result);
    }
    return std::make_pair(Status::OK(), result);
}

Status TableTestHelper::Merge(bool mergeAutoReload, StepInfo* step)
{
    return DoMerge(MergeOption::MergeAutoReadOption(mergeAutoReload), step);
}

std::pair<Status, framework::VersionCoord> TableTestHelper::OfflineMerge(const framework::VersionCoord& versionCoord,
                                                                         MergeOption option)
{
    auto helper = CreateMergeHelper();
    auto status = helper->Open(_indexRoot, _schema, _options, versionCoord);
    if (!status.IsOK()) {
        return std::make_pair(status, framework::VersionCoord(helper->GetCurrentVersion().GetVersionId()));
    }
    status = helper->Merge(option);
    return std::make_pair(status, framework::VersionCoord(helper->GetCurrentVersion().GetVersionId()));
}

Status TableTestHelper::Merge(MergeOption option)
{
    auto step = std::make_unique<StepInfo>();
    return DoMerge(option, step.get());
}
Status TableTestHelper::DoMerge(const MergeOption& option, StepInfo* step)
{
    Status status;
    StepInfo* stepInfo = step;
    std::unique_ptr<StepInfo> holder;
    if (stepInfo == NULL) {
        holder = std::make_unique<StepInfo>();
        stepInfo = holder.get();
    }
    while (true) {
        std::tie(status, *stepInfo) = StepMerge(*stepInfo, option);
        if (!status.IsOK()) {
            return status;
        }
        if (!option.mergeAutoReload && stepInfo->stepNum == 1) {
            return Status::OK();
        }
        if (stepInfo->isFinished) {
            return status;
        }
    }
    return Status::OK();
}

std::pair<Status, TableTestHelper::StepInfo> TableTestHelper::StepMerge(StepInfo step, const MergeOption& option)
{
    StepInfo result = step;
    auto mergeController =
        (LocalTabletMergeControllerForTest*)framework::TabletTestAgent(GetTablet()).TEST_GetMergeController().get();
    mergeController->SetTaskPlan(step.taskPlan);
    mergeController->TEST_SetSpecifyEpochId(step.specifyEpochId);
    mergeController->TEST_SetSpecifyMaxMergedSegIdAndVersionId(step.specifyMaxMergedSegId,
                                                               step.specifyMaxMergedVersionId);
    if (step.stepNum < 1) {
        auto status = ExecuteMerge(option);
        result.taskPlan = mergeController->GetTaskPlan();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "Execute inner task failed");
            return std::make_pair(status, result);
        }
        if (!GetTablet()->NeedCommit()) {
            AUTIL_LOG(ERROR, "step merge not need commit");
            return std::make_pair(Status::Unimplement("need not commit"), result);
        }
        result.stepNum = 1;
        return std::make_pair(Status::OK(), result);
    }

    if (step.stepNum < 2) {
        if (!GetTablet()->NeedCommit()) {
            AUTIL_LOG(ERROR, "step merge not need commit");
            return std::make_pair(Status::Unimplement("need not commit"), result);
        }
        auto [status, versionMeta] = GetTablet()->Commit(framework::CommitOptions().SetNeedPublish(true));
        if (!status.IsOK()) {
            return std::make_pair(status, result);
        }
        result.stepNum = 2;
        result.stepResultVersion = versionMeta.GetVersionId();
        return std::make_pair(Status::OK(), result);
    }
    if (step.stepNum < 3) {
        auto status = Reopen(framework::ReopenOptions(), step.stepResultVersion);
        if (!status.IsOK()) {
            return {status, result};
        }
        result.stepNum = 3;
        result.isFinished = true;
        return std::make_pair(Status::OK(), result);
    }
    return std::make_pair(Status::OK(), result);
}

void TableTestHelper::Close()
{
    const auto& tablet = GetTablet();
    if (tablet) {
        return tablet->Close();
    }
}

bool TableTestHelper::Deploy(const framework::IndexRoot& indexRoot, const config::TabletOptions* tabletOptions,
                             versionid_t baseVersionId, versionid_t targetVersionId)
{
    const bool TRACE = false;
    framework::TabletDeployer::GetDeployIndexMetaInputParams inputParams;
    inputParams.rawPath = indexRoot.GetRemoteRoot();
    inputParams.remotePath = indexRoot.GetRemoteRoot();
    inputParams.localPath = indexRoot.GetLocalRoot();
    inputParams.baseTabletOptions = tabletOptions;
    inputParams.targetTabletOptions = tabletOptions;
    inputParams.baseVersionId = baseVersionId;
    inputParams.targetVersionId = targetVersionId;
    framework::TabletDeployer::GetDeployIndexMetaOutputParams outputParams;
    if (!framework::TabletDeployer::GetDeployIndexMeta(inputParams, &outputParams)) {
        return false;
    }
    auto trace = [](const std::string& name, const indexlib::file_system::DeployIndexMetaVec& deployIndexMetaVec) {
        std::cout << "DEPLOY: " << name << "[" << deployIndexMetaVec.size() << "]" << std::endl;
        for (const auto& deployIndexMeta : deployIndexMetaVec) {
            auto sourceRootPath = deployIndexMeta->sourceRootPath;
            auto targetRootPath = deployIndexMeta->targetRootPath;
            std::cout << "[" << sourceRootPath << "] --> [" << targetRootPath << "]"
                      << "[" << deployIndexMeta->deployFileMetas.size() << ":"
                      << deployIndexMeta->finalDeployFileMetas.size() << "]" << std::endl;
            for (const auto& fileInfo : deployIndexMeta->deployFileMetas) {
                std::cout << "    " << std::left << std::setw(120) << fileInfo.filePath << std::right << std::setw(10)
                          << fileInfo.fileLength << std::endl;
            }
            for (const auto& fileInfo : deployIndexMeta->finalDeployFileMetas) {
                std::cout << "F   " << std::left << std::setw(120) << fileInfo.filePath << std::right << std::setw(10)
                          << fileInfo.fileLength << std::endl;
            }
        }
    };
    if (TRACE) {
        trace("local", outputParams.localDeployIndexMetaVec);
        trace("remote", outputParams.remoteDeployIndexMetaVec);
    }

    auto deploy = [&indexRoot](const indexlib::file_system::DeployIndexMetaVec& deployIndexMetaVec) -> bool {
        (void)indexRoot;
        for (const auto& deployIndexMeta : deployIndexMetaVec) {
            for (const auto& fileInfo : deployIndexMeta->deployFileMetas) {
                assert(deployIndexMeta->sourceRootPath ==
                       indexlib::util::PathUtil::NormalizePath(indexRoot.GetRemoteRoot()));
                auto destPath = indexlib::util::PathUtil::JoinPath(deployIndexMeta->targetRootPath, fileInfo.filePath);
                if (fileInfo.isDirectory()) {
                    if (!indexlib::file_system::FslibWrapper::MkDir(destPath, true, true).OK()) {
                        return false;
                    }
                } else {
                    auto srcPath =
                        indexlib::util::PathUtil::JoinPath(deployIndexMeta->sourceRootPath, fileInfo.filePath);
                    if (auto ec = indexlib::file_system::FslibWrapper::Copy(srcPath, destPath);
                        ec != indexlib::file_system::FSEC_OK && ec != indexlib::file_system::FSEC_EXIST) {
                        return false;
                    }
                }
            }
            for (const auto& fileInfo : deployIndexMeta->finalDeployFileMetas) {
                assert(fileInfo.isFile());
                assert(deployIndexMeta->sourceRootPath ==
                       indexlib::util::PathUtil::NormalizePath(indexRoot.GetRemoteRoot()));
                auto srcPath = indexlib::util::PathUtil::JoinPath(deployIndexMeta->sourceRootPath, fileInfo.filePath);
                auto destPath = indexlib::util::PathUtil::JoinPath(deployIndexMeta->targetRootPath, fileInfo.filePath);
                if (auto ec = indexlib::file_system::FslibWrapper::Copy(srcPath, destPath);
                    ec != indexlib::file_system::FSEC_OK && ec != indexlib::file_system::FSEC_EXIST) {
                    return false;
                }
            }
        }
        return true;
    };
    // version in root will deploy by suez
    auto deployRootVersion = [targetVersionId, &indexRoot]() -> bool {
        std::string versionFileName = PathUtil::GetVersionFileName(targetVersionId);
        auto srcPath = indexlib::util::PathUtil::JoinPath(indexRoot.GetRemoteRoot(), versionFileName);
        auto destPath = indexlib::util::PathUtil::JoinPath(indexRoot.GetLocalRoot(), versionFileName);
        if (auto ec = indexlib::file_system::FslibWrapper::Copy(srcPath, destPath);
            ec != indexlib::file_system::FSEC_OK && ec != indexlib::file_system::FSEC_EXIST) {
            return false;
        }
        return true;
    };
    if (indexRoot.GetLocalRoot() != indexRoot.GetRemoteRoot()) {
        if (!deploy(outputParams.localDeployIndexMetaVec) || !deployRootVersion()) {
            return false;
        }
    }
    return WriteDpDoneFile(indexRoot, targetVersionId, ToJsonString(outputParams.versionDeployDescription));
}

bool TableTestHelper::Query(std::string indexType, std::string indexName, std::string queryStr, std::string expectValue)
{
    return DoQuery(indexType, indexName, queryStr, expectValue);
}

TableTestHelper& TableTestHelper::SetExecutor(future_lite::Executor* dumpExecutor,
                                              future_lite::TaskScheduler* taskScheduler)
{
    _impl->tabletResource.dumpExecutor = dumpExecutor;
    _impl->tabletResource.taskScheduler = taskScheduler;
    return *this;
}

TableTestHelper&
TableTestHelper::SetMemoryQuotaController(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
                                          const std::shared_ptr<MemoryQuotaController>& buildMemoryQuotaController)
{
    _impl->tabletResource.memoryQuotaController = memoryQuotaController;
    _impl->tabletResource.buildMemoryQuotaController = buildMemoryQuotaController;
    return *this;
}

TableTestHelper& TableTestHelper::SetFileBlockCacheContainer(
    const std::shared_ptr<indexlib::file_system::FileBlockCacheContainer>& fileBlockCacheContainer)
{
    _impl->tabletResource.fileBlockCacheContainer = fileBlockCacheContainer;
    return *this;
}

TableTestHelper&
TableTestHelper::SetMergeController(const std::shared_ptr<framework::ITabletMergeController>& mergeController)
{
    _impl->tabletResource.mergeController = mergeController;
    return *this;
}

TableTestHelper& TableTestHelper::SetIdGenerator(const std::shared_ptr<framework::IdGenerator>& idGenerator)
{
    _impl->tabletResource.idGenerator = idGenerator;
    return *this;
}

TableTestHelper& TableTestHelper::SetSearchCache(const std::shared_ptr<indexlib::util::SearchCache>& searchCache)
{
    _impl->tabletResource.searchCache = searchCache;
    return *this;
}

static std::shared_ptr<config::TabletOptions> MakeTabletOptions(bool isOnline, int32_t makeTabletOptionsFlags)
{
    auto options = std::make_shared<config::TabletOptions>();
    options->SetIsOnline(isOnline);
    options->SetIsLeader(makeTabletOptionsFlags & TOF_LEADER);
    options->SetFlushLocal(makeTabletOptionsFlags & TOF_FLUSH_LOCAL);
    options->SetFlushRemote(makeTabletOptionsFlags & TOF_FLUSH_REMOTE);
    if (makeTabletOptionsFlags & TOF_DISABLE_BACKGROUD_TASK) {
        options->MutableBackgroundTaskConfig().DisableAll();
    }
    return options;
}

std::shared_ptr<config::TabletOptions> TableTestHelper::MakeOnlineTabletOptions(int32_t makeTabletOptionsFlags)
{
    return MakeTabletOptions(/*isOnline*/ true, makeTabletOptionsFlags);
}

std::shared_ptr<config::TabletOptions> TableTestHelper::MakeOfflineTabletOptions(int32_t makeTabletOptionsFlags)
{
    return MakeTabletOptions(/*isOnline*/ false, makeTabletOptionsFlags);
}

framework::ReopenOptions TableTestHelper::GetReopenOptions(framework::OpenOptions::BuildMode buildMode)
{
    indexlibv2::framework::OpenOptions openOptions;
    openOptions.SetBuildMode(buildMode);
    openOptions.SetUpdateControlFlowOnly(true);
    return indexlibv2::framework::ReopenOptions(openOptions);
}

bool TableTestHelper::WriteDpDoneFile(const framework::IndexRoot& indexRoot, versionid_t versionId,
                                      const std::string& content)
{
    auto doneFile = indexlib::file_system::FslibWrapper::JoinPath(
        indexRoot.GetLocalRoot(), framework::VersionDeployDescription::GetDoneFileNameByVersionId(versionId));
    bool exist = false;
    if (!fslib::util::FileUtil::isFile(doneFile, exist)) {
        return false;
    }
    if (exist && !fslib::util::FileUtil::remove(doneFile)) {
        return false;
    }
    if (!fslib::util::FileUtil::writeFile(doneFile, content)) {
        return false;
    }
    return true;
}

} // namespace indexlibv2::table
