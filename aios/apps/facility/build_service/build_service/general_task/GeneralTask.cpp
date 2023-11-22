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
#include "build_service/general_task/GeneralTask.h"

#include <algorithm>
#include <assert.h>
#include <chrono>
#include <cstdint>
#include <google/protobuf/stubs/port.h>
#include <iterator>
#include <optional>
#include <thread>
#include <type_traits>
#include <typeinfo>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/legacy/base64.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/common/IndexReclaimParamPreparer.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/reader/SourceFieldExtractorDocIterator.h"
#include "build_service/util/MemUtil.h"
#include "build_service/util/Monitor.h"
#include "future_lite/ExecutorCreator.h"
#include "future_lite/Try.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/analyzer/IAnalyzerFactory.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/FileBlockCache.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/framework/EnvironmentVariablesProvider.h"
#include "indexlib/framework/ITabletDocIterator.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/MetricsWrapper.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletFactoryCreator.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/framework/index_task/CustomIndexTaskFactory.h"
#include "indexlib/framework/index_task/IndexTaskContextCreator.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/util/TaskItem.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "kmonitor/client/MetricType.h"

CHECK_FUTURE_LITE_EXECUTOR(async_io);

namespace build_service::task_base {

BS_LOG_SETUP(task_base, GeneralTask);

const std::string GeneralTask::TASK_NAME = "general_task";
const std::string GeneralTask::CHECKPOINT_NAME = "__GENERAL_CKP__";
const std::string GeneralTask::DEFAULT_GLOBAL_BLOCK_CACHE_PARAM_TEMPLATE =
    "memory_size_in_mb=$MEMORY_SIZE_IN_MB$;block_size=2097152;io_batch_size=1;num_shard_bits=0";

using namespace std::chrono_literals;

GeneralTask::GeneralTaskMetrics::GeneralTaskMetrics(const std::shared_ptr<kmonitor::MetricsTags>& metricsTags,
                                                    const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter)
    : _metricsTags(metricsTags)
    , _metricsReporter(metricsReporter)
    , _operationCount(0)
{
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, report, "general_task/pendingAndRunningOpCount",
                                         kmonitor::GAUGE);
}

GeneralTask::GeneralTaskMetrics::~GeneralTaskMetrics() {}

void GeneralTask::GeneralTaskMetrics::ReportMetrics()
{
    if (_reportMetric) {
        _reportMetric->Report(_metricsTags.get(), _operationCount);
    }
}

void GeneralTask::GeneralTaskMetrics::RegisterMetrics() {}

GeneralTask::~GeneralTask()
{
    if (_workLoopThread) {
        _workLoopThread->stop();
        _workLoopThread.reset();
    }

    // wait all op finish before engine stopped.
    while (hasRunningOps()) {
        BS_INTERVAL_LOG(100, WARN, "wait running op finish");
        std::this_thread::sleep_for(100ms);
        // TODO: cancel running ops
        if (_generalTaskMetrics) {
            _generalTaskMetrics->UpdateOperationCount(TEST_getPendingAndRunningOpSize());
        }
        if (_metricsManager) {
            _metricsManager->ReportMetrics();
        }
    }
    BS_LOG(INFO, "general task exited");
}

bool GeneralTask::init(Task::TaskInitParam& initParam)
{
    _initParam = initParam;
    initExecuteEpochId();
    updateCurrent();
    return true;
}

void GeneralTask::initExecuteEpochId()
{
    const auto& address = _initParam.address;
    if (address.has_ip() && address.has_arpcport() && address.has_starttimestamp()) {
        _executeEpochId =
            address.ip() + "_" + std::to_string(address.arpcport()) + "_" + std::to_string(address.starttimestamp());
    } else {
        _executeEpochId = _initParam.epochId;
    }
    BS_LOG(INFO, "execute epoch id set as [%s]", _executeEpochId.c_str());
}

bool GeneralTask::prepare(const std::string taskName, const std::string taskType, const config::TaskTarget& target)
{
    _tabletSchema = _initParam.resourceReader->getTabletSchema(_initParam.clusterName);
    if (!_tabletSchema) {
        BS_LOG(ERROR, "get schema for cluster[%s] failed", _initParam.clusterName.c_str());
        return false;
    }
    _tabletOptions = _initParam.resourceReader->getTabletOptions(_initParam.clusterName);
    if (!_tabletOptions) {
        BS_LOG(ERROR, "init tablet options [%s] failed", _initParam.clusterName.c_str());
        return false;
    }
    auto taskConfig = _tabletOptions->GetIndexTaskConfig(taskType, taskName);
    if (taskConfig != std::nullopt) {
        auto [status, num] = taskConfig->GetSetting<uint32_t>(config::BS_TASK_THREAD_NUM);
        if (status.IsOK()) {
            _threadNum = num;
        }
    }

    _tabletOptions->SetIsOnline(false);
    _tabletFactory = indexlibv2::framework::TabletFactoryCreator::GetInstance()->Create(_tabletSchema->GetTableType());
    if (!_tabletFactory) {
        BS_LOG(
            ERROR, "create tablet factory with type[%s] failed, registered types [%s]",
            _tabletSchema->GetTableType().c_str(),
            autil::legacy::ToJsonString(indexlibv2::framework::TabletFactoryCreator::GetInstance()->GetRegisteredType())
                .c_str());
        return false;
    }
    std::string indexPath;
    if (!target.getTargetDescription(config::BS_GENERAL_TASK_PARTITION_INDEX_ROOT, indexPath)) {
        BS_LOG(ERROR, "get [%s] failed", config::BS_GENERAL_TASK_PARTITION_INDEX_ROOT.c_str());
        return false;
    }
    auto options = std::make_shared<indexlibv2::config::TabletOptions>(*_tabletOptions);

    if (!_tabletFactory->Init(options, nullptr)) {
        BS_LOG(ERROR, "init tablet factory failed");
        return false;
    }

    auto status = indexlibv2::framework::TabletSchemaLoader::ResolveSchema(options, indexPath, _tabletSchema.get());
    if (!status.IsOK()) {
        BS_LOG(ERROR, "resolve schema failed: %s", status.ToString().c_str());
        return false;
    }

    if (!initIndexTaskContextCreator(taskName, taskType, target)) {
        BS_LOG(ERROR, "init index task context creator for tablet[%s][%s] failed",
               _tabletSchema->GetTableType().c_str(), _tabletSchema->GetTableName().c_str());
        return false;
    }
    if (!initEngine()) {
        BS_LOG(ERROR, "create engine failed");
        return false;
    }
    if (!startWorkLoop()) {
        BS_LOG(ERROR, "start loop thread failed");
        return false;
    }

    BS_LOG(INFO, "tablet[%s][%s] inited for task epoch[%s]", _tabletSchema->GetTableType().c_str(),
           _tabletSchema->GetTableName().c_str(), _executeEpochId.c_str());
    return true;
}

bool GeneralTask::startWorkLoop()
{
    auto loopInteralInMs = autil::EnvUtil::getEnv<int64_t>("bs_general_task_work_loop_interval_ms", /*default*/ 100);
    _workLoopThread = autil::LoopThread::createLoopThread([this]() { workLoop(); }, loopInteralInMs * 1000,
                                                          /*name*/ "GeneralTaskWorkLoop");
    return _workLoopThread.get();
}

bool GeneralTask::initIndexTaskContextCreator(const std::string taskName, const std::string taskType,
                                              const config::TaskTarget& target)
{
    uint64_t machineTotalMem = util::MemUtil::getMachineTotalMem();
    static constexpr double defaultAllowRatio = 0.95;
    auto allowRatio = autil::EnvUtil::getEnv<double>("bs_general_task_memory_allow_ratio", defaultAllowRatio);
    if (allowRatio <= 0 or allowRatio > 1.0) {
        BS_LOG(WARN, "bs_general_task_memory_allow_ratio [%lf] is not valid, set to %lf", allowRatio,
               defaultAllowRatio);
        allowRatio = defaultAllowRatio;
    }
    int64_t memoryQuota = static_cast<int64_t>(machineTotalMem * allowRatio);
    _availableMemory = memoryQuota;
    std::string taskEpochId;
    if (!target.getTargetDescription(config::BS_GENERAL_TASK_EPOCH_ID, taskEpochId)) {
        BS_LOG(ERROR, "get [%s] failed", config::BS_GENERAL_TASK_EPOCH_ID.c_str());
        return false;
    }
    auto memoryQuotaController =
        std::make_shared<indexlibv2::MemoryQuotaController>(_tabletSchema->GetTableName(), memoryQuota);
    _metricsManager.reset(new indexlibv2::framework::MetricsManager(
        _tabletSchema->GetTableName(), _initParam.metricProvider ? _initParam.metricProvider->GetReporter() : nullptr));
    _contextCreator = std::make_unique<indexlibv2::framework::IndexTaskContextCreator>(_metricsManager);
    if (_contextCreator == nullptr) {
        BS_LOG(ERROR, "create index context creator fail.");
        return false;
    }

    initGeneralTaskMetrics();
    _contextCreator->SetTabletName(_tabletSchema->GetTableName())
        .SetTabletSchema(_tabletSchema)
        .SetTabletOptions(_tabletOptions)
        .SetTaskEpochId(taskEpochId)
        .SetExecuteEpochId(_executeEpochId)
        .SetTabletFactory(_tabletFactory.get())
        .SetMemoryQuotaController(memoryQuotaController)
        .SetMetricProvider(_initParam.metricProvider);
    _contextCreator->SetDesignateTask(taskType, taskName);
    std::string reservedVersionsStr;
    if (target.getTargetDescription(indexlibv2::table::RESERVED_VERSION_COORD_SET, reservedVersionsStr)) {
        if (!reservedVersionsStr.empty()) {
            _contextCreator->AddParameter(indexlibv2::table::RESERVED_VERSION_COORD_SET, reservedVersionsStr);
        }
    }

    std::string blockCacheParam;
    auto envProvider = _tabletFactory->CreateEnvironmentVariablesProvider();
    std::string defaultBlockCacheParam = DEFAULT_GLOBAL_BLOCK_CACHE_PARAM_TEMPLATE;
    autil::StringUtil::replaceAll(defaultBlockCacheParam, "$MEMORY_SIZE_IN_MB$",
                                  std::to_string(std::min(4096u, _threadNum * 512)));
    if (envProvider != nullptr) {
        blockCacheParam = envProvider->Get("BS_BLOCK_CACHE", defaultBlockCacheParam);
    } else {
        blockCacheParam = autil::EnvUtil::getEnv<std::string>(
            /*key*/ "BS_BLOCK_CACHE", /*defaultValue*/ defaultBlockCacheParam);
    }
    auto fileBlockCacheContainer = std::make_shared<indexlib::file_system::FileBlockCacheContainer>();
    if (!fileBlockCacheContainer->Init(blockCacheParam, memoryQuotaController,
                                       std::shared_ptr<indexlib::util::TaskScheduler>(), _initParam.metricProvider)) {
        BS_LOG(ERROR, "create file block cache failed, param: [%s]", blockCacheParam.c_str());
        return false;
    } else {
        BS_LOG(INFO, "create file block cache success, param: [%s]", blockCacheParam.c_str());
    }
    _contextCreator->SetFileBlockCacheContainer(fileBlockCacheContainer);

    std::string indexRoot;
    if (!target.getTargetDescription(config::BS_GENERAL_TASK_PARTITION_INDEX_ROOT, indexRoot)) {
        BS_LOG(ERROR, "get [%s] failed", config::BS_GENERAL_TASK_PARTITION_INDEX_ROOT.c_str());
        return false;
    }
    _contextCreator->SetDestDirectory(indexRoot);

    std::vector<std::pair<std::string, indexlibv2::versionid_t>> srcVersions;
    std::string content;
    if (!target.getTargetDescription(config::BS_GENERAL_TASK_SOURCE_VERSIONS, content)) {
        BS_LOG(INFO, "no [%s] in target, use dest index root as source index root",
               config::BS_GENERAL_TASK_SOURCE_VERSIONS.c_str());
        srcVersions.push_back({indexRoot, /*versionId*/ indexlibv2::INVALID_VERSIONID});
    } else {
        try {
            autil::legacy::FromJsonString(srcVersions, content);
        } catch (const autil::legacy::ExceptionBase& e) {
            BS_LOG(ERROR, "parse [%s] failed, exception [%s]", config::BS_GENERAL_TASK_SOURCE_VERSIONS.c_str(),
                   e.what());
            return false;
        }
    }

    auto baseVersionId = indexlibv2::INVALID_VERSIONID;
    assert(srcVersions.size() == 1u);
    for (const auto& [srcRoot, srcVersionId] : srcVersions) {
        _contextCreator->AddSourceVersion(srcRoot, srcVersionId);
        baseVersionId = srcVersionId;
    }
    BS_LOG(INFO, "task context: task epochId[%s], worker execute epochId[%s], base version[%d]", taskEpochId.c_str(),
           _executeEpochId.c_str(), baseVersionId);
    return true;
}

std::unique_ptr<indexlibv2::framework::IIndexOperationCreator>
GeneralTask::getCustomOperationCreator(const indexlibv2::config::CustomIndexTaskClassInfo& customClassInfo,
                                       const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema)
{
    auto customIndexTaskFactoryCreator = indexlibv2::framework::CustomIndexTaskFactoryCreator::GetInstance();
    auto className = customClassInfo.GetClassName();
    auto factory = customIndexTaskFactoryCreator->Create(className);
    if (!factory) {
        BS_LOG(ERROR, "no task factory for [%s], registed typeds [%s]", className.c_str(),
               autil::legacy::ToJsonString(customIndexTaskFactoryCreator->GetRegisteredType(), true).c_str());
        return nullptr;
    }
    return factory->CreateIndexOperationCreator(schema);
}

bool GeneralTask::initEngine()
{
    auto executorType = autil::EnvUtil::getEnv("bs_general_task_executor_type", std::string("async_io"));
    _executor = future_lite::ExecutorCreator::Create(
        executorType,
        future_lite::ExecutorCreator::Parameters().SetExecutorName("op_execute_engine").SetThreadNum(_threadNum));
    if (!_executor) {
        BS_LOG(ERROR, "create executor[%s] failed", executorType.c_str());
        return false;
    }
    auto context = createIndexTaskContext();
    if (!context) {
        BS_LOG(ERROR, "create context failed");
        return false;
    }
    const auto& schema = context->GetTabletData()->GetWriteSchema();
    auto opCreator = _tabletFactory->CreateIndexOperationCreator(schema);
    auto designateTaskConfig = context->GetDesignateTaskConfig();
    if (designateTaskConfig) {
        auto [status, customOpCreator] =
            indexlibv2::framework::CustomIndexTaskFactory::GetCustomOperationCreator(designateTaskConfig, schema);
        if (!status.IsOK()) {
            return false;
        }
        if (customOpCreator) {
            opCreator = std::move(customOpCreator);
        }
    }
    if (opCreator == nullptr) {
        BS_LOG(ERROR, "create op creator failed");
        return false;
    }
    BS_LOG(INFO, "LocalExecuteEngine created, executor[%s], threadNum[%d]", executorType.c_str(), _threadNum);
    _engine = std::make_unique<indexlibv2::framework::LocalExecuteEngine>(_executor.get(), std::move(opCreator));
    return true;
}

void GeneralTask::unsafeUpdateOpDetail(const std::vector<std::shared_ptr<GeneralTask::OperationDetail>>& toRuns,
                                       const std::vector<std::shared_ptr<GeneralTask::OperationDetail>>& toRemoves,
                                       OpDetailMap* opDetails)
{
    for (auto& toRemove : toRemoves) {
        auto opId = toRemove->desc.GetId();
        auto iter = (*opDetails).find(opId);
        if (iter != (*opDetails).end()) {
            iter->second->isRemoved = true;
        }
    }
    for (auto& toRun : toRuns) {
        auto opId = toRun->desc.GetId();
        auto iter = (*opDetails).find(opId);
        if (iter != (*opDetails).end()) {
            iter->second->isRemoved = false;
            BS_LOG(ERROR, "attempted to run a removed op[%s][%ld], reset isRemoved to false",
                   toRun->desc.GetType().c_str(), opId);
            continue;
        }
        BS_LOG(INFO, "add op[%s][%ld]", toRun->desc.GetType().c_str(), opId);
        toRun->result.set_id(opId);
        toRun->result.set_status(proto::OP_PENDING);
        toRun->result.set_resultinfo("");
        (*opDetails)[opId] = toRun;
    }
}

bool GeneralTask::handleTarget(const config::TaskTarget& target)
{
    std::string content;
    if (!target.getTargetDescription(config::BS_GENERAL_TASK_OPERATION_TARGET, content)) {
        BS_LOG(INFO, "no target description[%s] found", config::BS_GENERAL_TASK_OPERATION_TARGET.c_str());
        return true;
    }
    proto::OperationTarget opTarget;
    if (!proto::ProtoUtil::parseBase64EncodedPbStr(content, &opTarget)) {
        BS_LOG(ERROR, "parse general task operation target failed. content[%s]", content.c_str());
        return false;
    }
    if (!_engine) {
        if (!prepare(opTarget.taskname(), opTarget.tasktype(), target)) {
            return false;
        }
    }
    assert(_engine);
    std::vector<std::shared_ptr<OperationDetail>> newOps = getOperationDetails(opTarget);
    std::vector<std::shared_ptr<OperationDetail>> oldOps;
    {
        std::lock_guard<std::mutex> lock(_opDetailsMutex);
        for (const auto& [opId, opDetail] : _opDetails) {
            if (opDetail->isRemoved) {
                continue;
            }
            oldOps.push_back(opDetail);
        }
    }
    auto [toRuns, toRemoves] = generateTodoOps(newOps, oldOps);
    {
        std::lock_guard<std::mutex> lock(_opDetailsMutex);
        unsafeUpdateOpDetail(toRuns, toRemoves, &_opDetails);
    }
    updateCurrent();
    return true;
}

std::vector<std::shared_ptr<GeneralTask::OperationDetail>>
GeneralTask::getOperationDetails(const proto::OperationTarget& opTarget) const
{
    std::vector<std::shared_ptr<OperationDetail>> ops;
    for (const auto& op : opTarget.ops()) {
        indexlibv2::framework::IndexOperationDescription desc(op.id(), op.type());
        desc.SetEstimateMemory(op.memory());
        for (auto depend : op.depends()) {
            desc.AddDepend(depend);
        }
        desc.ClearParameters();
        for (size_t i = 0; i < op.paramkeys_size(); ++i) {
            const auto& paramKey = op.paramkeys(i);
            const auto& paramVal = op.paramvalues(i);
            desc.AddParameter(paramKey, paramVal);
        }
        auto opDetail = std::make_shared<OperationDetail>();
        opDetail->desc = std::move(desc);
        if (!op.dependworkerepochids().empty()) {
            assert(op.dependworkerepochids_size() == op.depends_size());
            for (const auto& epochId : op.dependworkerepochids()) {
                opDetail->dependOpExecuteEpochIds.push_back(epochId);
            }
        }
        ops.push_back(opDetail);
    }
    return ops;
}

std::pair</*toRun=*/std::vector<std::shared_ptr<GeneralTask::OperationDetail>>,
          /*toRemove=*/std::vector<std::shared_ptr<GeneralTask::OperationDetail>>>
GeneralTask::generateTodoOps(std::vector<std::shared_ptr<GeneralTask::OperationDetail>> newDetails,
                             std::vector<std::shared_ptr<GeneralTask::OperationDetail>> oldDetails)
{
    auto cmp = [](const std::shared_ptr<OperationDetail>& lhs, const std::shared_ptr<OperationDetail>& rhs) {
        return lhs->desc.GetId() < rhs->desc.GetId();
    };

    std::sort(newDetails.begin(), newDetails.end(), cmp);
    std::sort(oldDetails.begin(), oldDetails.end(), cmp);
    std::vector<std::shared_ptr<OperationDetail>> toRuns;

    std::set_difference(newDetails.begin(), newDetails.end(), oldDetails.begin(), oldDetails.end(),
                        std::inserter(toRuns, toRuns.end()), cmp);
    std::vector<std::shared_ptr<OperationDetail>> toRemoves;
    std::set_difference(oldDetails.begin(), oldDetails.end(), newDetails.begin(), newDetails.end(),
                        std::inserter(toRemoves, toRemoves.end()), cmp);
    return {std::move(toRuns), std::move(toRemoves)};
}

std::unique_ptr<indexlibv2::framework::IndexTaskContext>
GeneralTask::createIndexTaskContext(const GeneralTask::OperationDetail& detail)
{
    if (!detail.dependOpExecuteEpochIds.empty()) {
        std::map<int64_t, std::string> epochIds;
        const auto& depends = detail.desc.GetDepends();
        for (size_t i = 0; i < detail.dependOpExecuteEpochIds.size(); ++i) {
            epochIds[depends[i]] = detail.dependOpExecuteEpochIds[i];
        }
        if (_contextCreator) {
            _contextCreator->SetFinishedOpExecuteEpochIds(epochIds, detail.desc.UseOpFenceDir());
        }
    }
    return createIndexTaskContext();
}

std::unique_ptr<indexlibv2::framework::IndexTaskContext> GeneralTask::createIndexTaskContext()
{
    return _contextCreator ? _contextCreator->CreateContext() : nullptr;
}

std::vector<std::shared_ptr<indexlibv2::framework::IndexTaskResource>>
GeneralTask::prepareExtendResource(const std::shared_ptr<config::ResourceReader>& resourceReader,
                                   const std::string& clusterName)
{
    std::vector<std::shared_ptr<indexlibv2::framework::IndexTaskResource>> indexTaskResources;
    auto CreateSourceFieldExtractorDocIter = [](const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema)
        -> std::shared_ptr<indexlibv2::framework::ITabletDocIterator> {
        return std::make_shared<reader::SourceFieldExtractorDocIterator>(schema);
    };
    auto docReaderIteratorCreatorRes = std::make_shared<
        indexlibv2::framework::ExtendResource<indexlibv2::framework::ITabletDocIterator::CreateDocIteratorFunc>>(
        indexlibv2::framework::DOC_READER_ITERATOR_CREATOR,
        std::make_shared<indexlibv2::framework::ITabletDocIterator::CreateDocIteratorFunc>(
            CreateSourceFieldExtractorDocIter));
    indexTaskResources.push_back(docReaderIteratorCreatorRes);
    std::shared_ptr<indexlibv2::analyzer::IAnalyzerFactory> analyzerFactory =
        analyzer::AnalyzerFactory::create(resourceReader);
    auto analyzerResource =
        std::make_shared<indexlibv2::framework::ExtendResource<indexlibv2::analyzer::IAnalyzerFactory>>(
            indexlibv2::framework::ANALYZER_FACTORY, analyzerFactory);
    indexTaskResources.push_back(analyzerResource);

    auto indexReclaimPreparer = std::make_shared<common::IndexReclaimParamPreparer>(clusterName);
    auto reclaimPrepareRes = std::make_shared<indexlibv2::framework::ExtendResource<indexlib::util::TaskItemWithStatus<
        const indexlibv2::framework::IndexTaskContext*, const std::map<std::string, std::string>&>>>(
        indexlibv2::framework::INDEX_RECLAIM_PARAM_PREPARER, indexReclaimPreparer);

    indexTaskResources.push_back(reclaimPrepareRes);
    auto resourceReaderResource = std::make_shared<indexlibv2::framework::ExtendResource<config::ResourceReader>>(
        config::BS_RESOURCE_READER, resourceReader);
    indexTaskResources.push_back(resourceReaderResource);
    return indexTaskResources;
}

indexlib::Status GeneralTask::addExtendResource(const indexlibv2::framework::IndexTaskContext* context) const
{
    std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager> resourceManager = context->GetResourceManager();
    if (resourceManager == nullptr) {
        BS_LOG(ERROR, "resource manager is nullptr");
        return indexlib::Status::InternalError("resource manager is nullptr");
    }
    auto extendResources = prepareExtendResource(_initParam.resourceReader, _initParam.clusterName);
    for (auto& extendResource : extendResources) {
        auto status = resourceManager->AddExtendResource(extendResource);
        if (!status.IsOK() && !status.IsExist()) {
            RETURN_IF_STATUS_ERROR(status, "add extend resource [%s] fail", extendResource->GetName().c_str());
        }
    }
    return indexlib::Status::OK();
}

void GeneralTask::workLoop()
{
    if (_generalTaskMetrics) {
        _generalTaskMetrics->UpdateOperationCount(TEST_getPendingAndRunningOpSize());
    }
    std::vector<std::shared_ptr<OperationDetail>> pendingOps;
    {
        std::lock_guard<std::mutex> lock(_opDetailsMutex);
        for (const auto& [id, detail] : _opDetails) {
            if (detail->result.status() == proto::OP_PENDING || detail->result.status() == proto::OP_ERROR) {
                pendingOps.push_back(detail);
            }
        }
    }
    std::sort(pendingOps.begin(), pendingOps.end(), [](const auto& lhs, const auto& rhs) {
        return lhs->desc.GetEstimateMemory() < rhs->desc.GetEstimateMemory();
    });

    for (auto& opDetail : pendingOps) {
        _availableMemory.fetch_sub(opDetail->desc.GetEstimateMemory());
        opDetail->result.set_status(proto::OP_RUNNING);
        BS_LOG(INFO, "run op[%s][%ld]", opDetail->desc.GetType().c_str(), opDetail->desc.GetId());
        auto context = createIndexTaskContext(*opDetail);
        assert(context);
        auto st = addExtendResource(context.get());
        if (!st.IsOK()) {
            BS_LOG(ERROR, "add extend resource failed.");
            return;
        }
        auto contextRawPtr = context.get();
        _engine->Schedule(opDetail->desc, contextRawPtr)
            .start([this, opDetail, context = std::move(context)](future_lite::Try<indexlib::Status>&& statusTry) {
                assert(!statusTry.hasError());
                auto& status = statusTry.value();
                if (status.IsOK()) {
                    BS_LOG(INFO, "op type[%s] id[%ld] finished", opDetail->desc.GetType().c_str(),
                           opDetail->desc.GetId());
                    opDetail->result.set_status(proto::OP_FINISHED);
                    opDetail->result.set_resultinfo(context->GetResult());
                } else {
                    BS_LOG(INFO, "op type[%s] id[%ld] error: %s", opDetail->desc.GetType().c_str(),
                           opDetail->desc.GetId(), status.ToString().c_str());
                    opDetail->result.set_status(proto::OP_ERROR);
                }
                _availableMemory.fetch_add(opDetail->desc.GetEstimateMemory());
            });
    }
    if (_metricsManager) {
        _metricsManager->ReportMetrics();
    }
    updateCurrent();
}

bool GeneralTask::isDone(const config::TaskTarget& target) { return false; }

indexlib::util::CounterMapPtr GeneralTask::getCounterMap() { return nullptr; }

void GeneralTask::updateCurrent()
{
    std::lock_guard<std::mutex> lock(_currentMutex);
    _current.set_workerepochid(_executeEpochId);
    _current.set_availablememory(_availableMemory.load());
    _current.clear_opresults();
    std::lock_guard<std::mutex> opLock(_opDetailsMutex);
    for (const auto& [id, detail] : _opDetails) {
        if (detail->isRemoved) {
            // skip detail that is marked as  isRemoved.
            continue;
        }
        *_current.add_opresults() = detail->result;
    }
}

std::string GeneralTask::getTaskStatus()
{
    std::lock_guard<std::mutex> lock(_currentMutex);
    std::string content;
    if (!_current.SerializeToString(&content)) {
        BS_LOG(ERROR, "serialize current failed");
        return "";
    }
    // for compatibility update package.
    // step 1: set bs admin env "ENABLE_OPTARGET_BASE64_ENCODE" to false (admin send TARGET with raw pb str)
    // step 2: update bs admin package (admin can recognize base64 encoded CURRENT)
    // step 3: update bs worker package (worker can recognize base64 encoded TARGET, and send base64 encoded CURRENT)
    // step 4: set bs admin env "ENABLE_OPTARGET_BASE64_ENCODE" to true (admin send base64 encoded TARGET)
    return autil::legacy::Base64EncodeFast(content);
}

size_t GeneralTask::TEST_getPendingAndRunningOpSize() const
{
    size_t ret = 0;
    std::lock_guard<std::mutex> lock(_opDetailsMutex);
    for (const auto& [id, detail] : _opDetails) {
        auto status = (detail->result).status();
        if (status == proto::OP_PENDING || status == proto::OP_RUNNING) {
            ++ret;
        }
    }
    return ret;
}

bool GeneralTask::hasRunningOps() const
{
    for (const auto& [id, detail] : _opDetails) {
        auto status = (detail->result).status();
        if (status == proto::OP_RUNNING) {
            return true;
        }
    }
    return false;
}

void GeneralTask::initGeneralTaskMetrics()
{
    if (!_metricsManager) {
        return;
    }
    auto tags = std::make_shared<kmonitor::MetricsTags>("clusterName", _initParam.clusterName);
    const std::string identifier = std::string("GENERAL_TASK_METRICS_") + typeid(*this).name() + "_" + _executeEpochId;
    const auto creatorFunc = [&]() -> std::shared_ptr<indexlibv2::framework::IMetrics> {
        return std::make_shared<GeneralTaskMetrics>(tags, _metricsManager->GetMetricsReporter());
    };
    _generalTaskMetrics =
        std::dynamic_pointer_cast<GeneralTaskMetrics>(_metricsManager->CreateMetrics(identifier, creatorFunc));
}

} // namespace build_service::task_base
