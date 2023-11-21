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
#include "build_service/admin/GenerationKeeper.h"

#include <assert.h>
#include <cstdint>
#include <exception>
#include <functional>
#include <ostream>
#include <type_traits>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "beeper/beeper.h"
#include "build_service/admin/AgentSimpleMasterScheduler.h"
#include "build_service/admin/GeneralGenerationTask.h"
#include "build_service/admin/GenerationTask.h"
#include "build_service/admin/GraphGenerationTask.h"
#include "build_service/admin/HippoProtoHelper.h"
#include "build_service/admin/JobTask.h"
#include "build_service/admin/catalog/CatalogVersionPublisher.h"
#include "build_service/admin/controlflow/TaskFlowManager.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/CpuSpeedEstimater.h"
#include "build_service/common/PathDefine.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/RoleNameGenerator.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "fslib/util/LongIntervalLog.h"
#include "hippo/proto/Common.pb.h"
#include "indexlib/framework/VersionCoord.h"
#include "master_framework/AppPlan.h"
#include "master_framework/RolePlan.h"
#include "worker_framework/LeaderElector.h"
#include "worker_framework/WorkerState.h"

using namespace std;

using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace build_service::proto;
using namespace build_service::util;
using namespace build_service::common;
using namespace build_service::config;
using fslib::fs::FileSystem;

using namespace worker_framework;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, GenerationKeeper);

#define SET_GENERATION_TASK(NEW_TASK)                                                                                  \
    {                                                                                                                  \
        ScopedLock lock(_generationMutex);                                                                             \
        _generationTask = NEW_TASK;                                                                                    \
        _generationTask->setCheckpointMetricReporter(_checkpointMetricReporter);                                       \
        _generationTask->setTaskStatusMetricReporter(_taskStatusMetricReporter);                                       \
        _generationTask->SetSlowNodeMetricReporter(_slowNodeMetricReporter);                                           \
        _generationTask->SetFatalErrorMetricReporter(_fatalErrorMetricReporter);                                       \
        if (_catalogPartitionIdentifier) {                                                                             \
            _generationTask->setCatalogPartitionIdentifier(_catalogPartitionIdentifier);                               \
        }                                                                                                              \
    }

const uint32_t GenerationKeeper::GENERATION_STATUS_SIZE_THRESHOLD = 1 * 1024 * 1024; // 1M

GenerationKeeper::GenerationKeeper(const Param& param)
    : _buildId(param.buildId)
    , _generationTask(NULL)
    , _zkRoot(param.zkRoot)
    , _adminServiceName(param.adminServiceName)
    , _amonitorPort(param.amonitorPort)
    , _generationDir(PathDefine::getGenerationZkRoot(param.zkRoot, param.buildId))
    , _zkWrapper(param.zkWrapper)
    , _zkStatus(param.zkWrapper,
                fslib::util::FileUtil::joinFilePath(_generationDir, PathDefine::ZK_GENERATION_STATUS_FILE))
    , _isStopped(false)
    , _needSyncStatus(false)
    , _workerTable(new WorkerTable(param.zkRoot, param.zkWrapper))
    , _prohibitIpCollector(param.prohibitIpCollector)
    , _startingBuildTimestamp(0)
    , _stoppingBuildTimestamp(0)
    , _refreshTimestamp(0)
    , _configPathChanged(false)
    , _reporter(param.monitor, false)
    , _catalogRpcChannelManager(param.catalogRpcChannelManager)
    , _catalogPartitionIdentifier(param.catalogPartitionIdentifier)
    , _specifiedWorkerPkgList(param.specifiedWorkerPkgList)
{
    setBeeperCollector(GENERATION_ERROR_COLLECTOR_NAME);
    initBeeperTag(param.buildId);

    if (EnvUtil::getEnv(BS_ENV_ENABLE_SCHEDULE_METRIC, false)) {
        _scheduleMetricReporter.reset(new ScheduleMetricReporter);
        _scheduleMetricReporter->init(_buildId, param.monitor);
    }
    _checkpointMetricReporter.reset(new CheckpointMetricReporter);
    _checkpointMetricReporter->init(_buildId, param.monitor);

    _taskStatusMetricReporter.reset(new TaskStatusMetricReporter);
    _taskStatusMetricReporter->init(_buildId, param.monitor);

    _slowNodeMetricReporter.reset(new SlowNodeMetricReporter);
    _slowNodeMetricReporter->Init(_buildId, param.monitor);

    if (_catalogPartitionIdentifier && _catalogRpcChannelManager) {
        _catalogVersionPublisher = std::make_unique<CatalogVersionPublisher>(
            _catalogPartitionIdentifier, _buildId.generationid(), _catalogRpcChannelManager);
    }
}

GenerationKeeper::~GenerationKeeper()
{
    if (_loopThreadPtr) {
        _loopThreadPtr->stop();
        _loopThreadPtr.reset();
    }
    DELETE_AND_SET_NULL(_generationTask);
    DELETE_AND_SET_NULL(_workerTable);
}

bool GenerationKeeper::recoverStoppedKeeper()
{
    string status;
    if (!readStopFile(status)) {
        return false;
    }
    if (!recoverGenerationTask(status)) {
        return false;
    }
    _isStopped = true;
    prepareCounterCollector(_generationTask->getCounterConfig());
    if (_jobCounterCollector) {
        _jobCounterCollector->disableSyncCounter();
    }
    return true;
}

bool GenerationKeeper::recover()
{
    string status;
    if (!readStatus(status)) {
        return false;
    }
    if (!recoverGenerationTask(status)) {
        return false;
    }

    bool validateExistingIndex = false;
    string errorMsg;
    if (!_generationTask->ValidateAndWriteTaskSignature(_generationDir, validateExistingIndex, &errorMsg)) {
        BS_LOG(INFO, "generation [%s] recover failed, validate task signature failed, errorMsg[%s]",
               _buildId.ShortDebugString().c_str(), errorMsg.c_str());
        return false;
    }
    auto identifier = _generationTask->getCatalogPartitionIdentifier();
    if (identifier && _catalogRpcChannelManager) {
        _catalogVersionPublisher =
            std::make_unique<CatalogVersionPublisher>(identifier, _buildId.generationid(), _catalogRpcChannelManager);
    }
    InformResponse response;
    initFromStatus(&response);
    prepareCounterCollector(_generationTask->getCounterConfig());
    return response.errormessage_size() == 0;
}

GenerationTaskBase* GenerationKeeper::innerRecoverGenerationTask(const string& status)
{
    return GenerationTaskBase::recover(_buildId, status, _zkWrapper);
}

bool GenerationKeeper::recoverGenerationTask(const string& status)
{
    GenerationTaskBase* task = innerRecoverGenerationTask(status);
    if (task == NULL || !task->loadFromString(status, _generationDir)) {
        DELETE_AND_SET_NULL(task);
        string errorMsg = "recover generation from [" + status + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    _needSyncStatus = false;
    GenerationTaskBase* tmp = _generationTask;
    SET_GENERATION_TASK(task);
    DELETE_AND_SET_NULL(tmp);
    // clean and fill workerTable, avoid workerTable is stale
    _workerTable->clearAllNodes();
    _generationTask->makeDecision(*_workerTable);
    return true;
}

bool GenerationKeeper::readStopFile(string& status)
{
    string stopFile = fslib::util::FileUtil::joinFilePath(_generationDir, PathDefine::ZK_GENERATION_STATUS_STOP_FILE);
    if (!fslib::util::FileUtil::readFile(stopFile, status)) {
        string errorMsg = "restart failed, status file: " + stopFile;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool GenerationKeeper::restart()
{
    string status;
    if (!readStopFile(status)) {
        return false;
    }

    DELETE_AND_SET_NULL(_generationTask);
    _generationTask = innerRecoverGenerationTask(status);
    if (_generationTask == NULL || !_generationTask->restart(status, _generationDir)) {
        string errorMsg = "restart failed, status file: [" + status + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        DELETE_AND_SET_NULL(_generationTask);
        return false;
    }
    _generationTask->setCheckpointMetricReporter(_checkpointMetricReporter);
    _generationTask->setTaskStatusMetricReporter(_taskStatusMetricReporter);
    _generationTask->SetSlowNodeMetricReporter(_slowNodeMetricReporter);
    _generationTask->SetFatalErrorMetricReporter(_fatalErrorMetricReporter);
    if (_catalogPartitionIdentifier) {
        _generationTask->setCatalogPartitionIdentifier(_catalogPartitionIdentifier);
    }

    _isStopped = false;
    return true;
}

void GenerationKeeper::startTask(const proto::StartTaskRequest* request, proto::InformResponse* response)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    auto snapshot = createTaskSnapshot();

    const BuildId& buildId = request->buildid();
    const string& taskName = request->taskname();
    string taskConfigPath;
    if (request->has_taskconfigpath()) {
        taskConfigPath = request->taskconfigpath();
    }
    int64_t taskId = request->taskid();
    string clusterName;
    if (request->has_clustername()) {
        clusterName = request->clustername();
    }

    string taskParam;
    if (request->has_taskparam()) {
        taskParam = request->taskparam();
    }

    string responseStr;
    GenerationTaskBase::StartTaskResponse taskResponse;
    if (!_generationTask->startTask(taskId, taskName, taskConfigPath, clusterName, taskParam, &taskResponse)) {
        BS_LOG(ERROR, "start task [%s] failed, error[%s]!", request->ShortDebugString().c_str(), responseStr.c_str());
        if (taskResponse.needRecover) {
            recoverGenerationTask(snapshot);
        }
        auto errorCode = taskResponse.duplicated ? ADMIN_DUPLICATE : ADMIN_INTERNAL_ERROR;
        SET_ERROR_AND_RETURN(buildId, response, errorCode, "start task [%s] failed, error[%d][%s]!",
                             request->ShortDebugString().c_str(), errorCode, responseStr.c_str());
    }

    if (!commitTaskStatus(snapshot)) {
        string errorMsg = "startTask failed: serialize generation status failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        recoverGenerationTask(snapshot);
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "start task [%s] failed, error[%s]!",
                             request->ShortDebugString().c_str(), errorMsg.c_str());
    }
    if (!responseStr.empty()) {
        response->set_response(responseStr);
    }
}

void GenerationKeeper::stopTask(const proto::StopTaskRequest* request, proto::InformResponse* response)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    auto snapshot = createTaskSnapshot();

    int64_t taskId = request->taskid();
    string errorMsg;
    if (!_generationTask->stopTask(taskId, errorMsg)) {
        BS_LOG(ERROR, "stop task [%s] failed, error[%s]!", request->ShortDebugString().c_str(), errorMsg.c_str());

        recoverGenerationTask(snapshot);
        SET_ERROR_AND_RETURN(request->buildid(), response, ADMIN_INTERNAL_ERROR, "stop task [%s] failed, error[%s]!",
                             request->ShortDebugString().c_str(), errorMsg.c_str());
    }

    if (!commitTaskStatus(snapshot)) {
        string errorMsg = "stop task failed : serialize generation status failed!";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        recoverGenerationTask(snapshot);
        SET_ERROR_AND_RETURN(request->buildid(), response, ADMIN_INTERNAL_ERROR, "stop task [%s] failed, error[%s]!",
                             request->ShortDebugString().c_str(), errorMsg.c_str());
    }
}

bool GenerationKeeper::getImportedVersionFromRequest(const proto::StartBuildRequest* request,
                                                     GenerationTaskBase::ImportedVersionIdMap* importedVersionIdMap)
{
    if (!request->has_extendparameters()) {
        BS_LOG(ERROR, "request does not have extendParameters");
        return false;
    }
    std::string extendParamStr = request->extendparameters();
    std::map<string, std::map<string, int32_t>> extendParamMap;
    try {
        FromJsonString(extendParamMap, extendParamStr);
    } catch (...) {
        BS_LOG(ERROR, "parse extendparameters [%s] failed", extendParamStr.c_str());
        return false;
    }
    auto iter = extendParamMap.find("importedVersions");
    if (iter == extendParamMap.end()) {
        BS_LOG(ERROR, "request does not have importedVersions");
        return false;
    }
    for (const auto& [clusterName, importedVersionId] : iter->second) {
        if (importedVersionId < 0) {
            BS_LOG(ERROR, "cluster [%s] has invalid importedVersionId [%d]", clusterName.c_str(), importedVersionId);
            return false;
        }
        (*importedVersionIdMap)[clusterName] = importedVersionId;
    }
    return true;
}

bool GenerationKeeper::importBuild(const string& configPath, const string& dataDescriptionKvs, const BuildId& buildId,
                                   const GenerationTaskBase::ImportedVersionIdMap& importedVersionIdMap,
                                   StartBuildResponse* response)
{
    DELETE_AND_SET_NULL(_generationTask);
    auto newTask = createGenerationTask(buildId, /*jobMode*/ false, /*buildMode*/ "");
    SET_GENERATION_TASK(newTask);
    if (!_generationTask->importBuild(configPath, _generationDir, dataDescriptionKvs, importedVersionIdMap, response)) {
        string errorMsg = "importBuild [" + buildId.ShortDebugString() + "] from generationTask failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        response->add_errormessage(errorMsg.c_str());
        return false;
    }
    return true;
}

GenerationTaskBase* GenerationKeeper::createGenerationTask(const proto::BuildId& buildId, bool jobMode,
                                                           const std::string& buildMode)
{
    if (buildMode == "customized") {
        return new GraphGenerationTask(buildId, _zkWrapper);
    } else if (buildMode == "general_task") {
        return new GeneralGenerationTask(buildId, _zkWrapper);
    }
    if (!jobMode) {
        return new GenerationTask(buildId, _zkWrapper);
    }
    return new JobTask(buildId, _zkWrapper);
}

void GenerationKeeper::startBuild(const proto::StartBuildRequest* request, proto::StartBuildResponse* response)
{
    const string& configPath = request->configpath();
    const string& dataDescriptionKvs = request->datadescriptionkvs();
    const BuildId& buildId = request->buildid();

    string buildMode;
    if (request->has_buildmode()) {
        buildMode = request->buildmode();
    }
    if (buildMode == "import") {
        GenerationTaskBase::ImportedVersionIdMap importedVersionIdMap;
        if (!getImportedVersionFromRequest(request, &importedVersionIdMap)) {
            SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                                 "import [%s] task does not have valid importedVersionId, extendParameters [%s]",
                                 buildId.ShortDebugString().c_str(),
                                 request->has_extendparameters() ? request->extendparameters().c_str() : "");
        }
        if (!importBuild(configPath, dataDescriptionKvs, buildId, importedVersionIdMap, response)) {
            SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "import task [%s] failed!",
                                 buildId.ShortDebugString().c_str());
        }
    } else {
        string stopFile =
            fslib::util::FileUtil::joinFilePath(_generationDir, PathDefine::ZK_GENERATION_STATUS_STOP_FILE);
        fslib::ErrorCode ec = FileSystem::isExist(stopFile);
        if (fslib::EC_TRUE != ec && fslib::EC_FALSE != ec) {
            SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "check [%s] isexist failed, error[%s]!",
                                 stopFile.c_str(), FileSystem::getErrorString(ec).c_str());
        }
        if (fslib::EC_TRUE == ec) {
            if (!restart()) {
                SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "restart generation [%s] failed",
                                     buildId.ShortDebugString().c_str());
            }
        } else {
            if (!cleanGenerationDir()) {
                SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                                     "start generation[%s] failed, clean generation dir",
                                     buildId.ShortDebugString().c_str());
            }
            bool jobMode = request->has_jobmode() && request->jobmode();

            DELETE_AND_SET_NULL(_generationTask);
            _generationTask = createGenerationTask(buildId, jobMode, buildMode);
            _generationTask->setCheckpointMetricReporter(_checkpointMetricReporter);
            _generationTask->setTaskStatusMetricReporter(_taskStatusMetricReporter);
            _generationTask->SetSlowNodeMetricReporter(_slowNodeMetricReporter);
            _generationTask->SetFatalErrorMetricReporter(_fatalErrorMetricReporter);
            if (_catalogPartitionIdentifier) {
                _generationTask->setCatalogPartitionIdentifier(_catalogPartitionIdentifier);
            }

            BuildStep buildStep = proto::BUILD_STEP_FULL;
            if (buildMode == config::BUILD_MODE_INCREMENTAL) {
                buildStep = proto::BUILD_STEP_INC;
                BS_LOG(INFO, "generation[%s] will start from incremental", buildId.ShortDebugString().c_str());

                string msg = "generation[" + buildId.ShortDebugString() + "] will start from incremental.";
                BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, msg, BuildIdWrapper::getEventTags(buildId));
            }
            if (!_generationTask->loadFromConfig(configPath, _generationDir, dataDescriptionKvs, buildStep)) {
                SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "start generation[%s] from config failed",
                                     buildId.ShortDebugString().c_str());
            }
            string errorMsg;
            bool validateExistingIndex = false;
            if (!_generationTask->ValidateAndWriteTaskSignature(_generationDir, validateExistingIndex, &errorMsg)) {
                BS_LOG(ERROR, "Validate and write task signature for generation[%s] failed, error message:[%s]",
                       buildId.ShortDebugString().c_str(), errorMsg.c_str());
                response->add_errormessage(errorMsg.c_str());
                ;
                return;
            }
        }
    }
    if (request->has_debugmode()) {
        string debugMode = request->debugmode();
        if (debugMode == "step") {
            _generationTask->setDebugMode(debugMode);
        }
    }

    string statusStr = ToJsonString(*_generationTask);
    if (!writeStatus(statusStr)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "start generation[%s] failed, "
                             "reason: serialize status to zk failed.",
                             buildId.ShortDebugString().c_str());
    }

    prepareCounterCollector(_generationTask->getCounterConfig());
    initFromStatus(response);
    if (response->errormessage_size() != 0) {
        return;
    }
    response->set_generationid(buildId.generationid());
    BS_LOG(INFO, "generation[%s] started.", buildId.ShortDebugString().c_str());
    string msg = "generation[" + buildId.ShortDebugString() + "] started.";
    BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, msg, BuildIdWrapper::getEventTags(buildId));
}

void GenerationKeeper::printGraph(const proto::PrintGraphRequest* request, proto::InformResponse* response)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    const BuildId& buildId = request->buildid();
    if (_isStopped) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "print graph [%s] failed : generation is stopped.", request->ShortDebugString().c_str());
    }

    bool printTask = false;
    if (request->has_printtaskinfo()) {
        printTask = request->printtaskinfo();
    }
    bool writeToLocal = false;
    if (request->has_writetolocal()) {
        writeToLocal = request->writetolocal();
    }
    string clusterName;
    if (request->has_clustername()) {
        clusterName = request->clustername();
    }
    string result;
    {
        ScopedLock lock(_decisionMutex);
        if (clusterName.empty()) {
            if (!_generationTask->printGraph(printTask, result)) {
                SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "print graph [%s] failed.",
                                     request->ShortDebugString().c_str());
            }
        } else {
            if (!_generationTask->printGraphForCluster(clusterName, printTask, result)) {
                SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                                     "print graph [%s] for cluster [%s] failed.", request->ShortDebugString().c_str(),
                                     clusterName.c_str());
            }
        }
    }

    if (!result.empty()) {
        response->set_response(result);
        if (writeToLocal) {
            string fileName = StringUtil::toString(buildId.generationid()) + "_" +
                              StringUtil::toString(TimeUtility::currentTimeInSeconds()) + ".dot";
            fslib::util::FileUtil::writeFile(fileName, result);
        }
    }
}

void GenerationKeeper::callGraph(const proto::CallGraphRequest* request, proto::InformResponse* response)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    const BuildId& buildId = request->buildid();
    if (_isStopped) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "call graph [%s] failed : generation is stopped.",
                             request->ShortDebugString().c_str());
    }

    KeyValueMap kvMap;
    if (!initGraphParameter(request, kvMap)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "call graph [%s] failed : invalid graph parameter.", request->ShortDebugString().c_str());
    }
    string graphFileName = request->graphfilename();
    string graphIdentifier = request->has_graphidentifier() ? request->graphidentifier() : "";

    ScopedLock lock(_decisionMutex);
    auto snapshot = createTaskSnapshot();
    if (!_generationTask->callGraph(graphIdentifier, graphFileName, kvMap)) {
        recoverGenerationTask(snapshot);
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "call graph [%s] failed : run callGraph fail.",
                             request->ShortDebugString().c_str());
    }

    if (!commitTaskStatus(snapshot)) {
        string errorMsg = "callGraph failed: serialize generation status failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        recoverGenerationTask(snapshot);
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "call graph [%s] failed : error [%s].",
                             request->ShortDebugString().c_str(), errorMsg.c_str());
    }
}

bool GenerationKeeper::stopBuild()
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);

    auto snapshot = createTaskSnapshot();
    _generationTask->stopBuild();

    if (!commitTaskStatus(snapshot)) {
        string errorMsg = "stop build failed : serialize generation status failed!";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        recoverGenerationTask(snapshot);
        return false;
    }
    return true;
}

bool GenerationKeeper::suspendBuild(const string& clusterName, string& errorMsg)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);

    auto snapshot = createTaskSnapshot();

    if (!_generationTask->suspendBuild(clusterName, errorMsg)) {
        BS_LOG(WARN, "suspend build failed");
        recoverGenerationTask(snapshot);
        return false;
    }
    if (!commitTaskStatus(snapshot)) {
        errorMsg = "suspend build failed : serialize generation status failed!";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        recoverGenerationTask(snapshot);
        return false;
    }
    return true;
}

bool GenerationKeeper::resumeBuild(const string& flowId, string& errorMsg)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);

    auto snapshot = createTaskSnapshot();
    if (!_generationTask->resumeBuild(flowId, errorMsg)) {
        BS_LOG(WARN, "resume build failed");
        recoverGenerationTask(snapshot);
        return false;
    }

    if (!commitTaskStatus(snapshot)) {
        errorMsg = "resume build failed : serialize generation status failed!";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        recoverGenerationTask(snapshot);
        return false;
    }
    return true;
}

bool GenerationKeeper::getBulkloadInfo(const std::string& clusterName, const std::string& bulkloadId,
                                       const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges,
                                       std::string* resultStr, std::string& errorMsg)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);

    auto snapshot = createTaskSnapshot();
    if (!_generationTask->getBulkloadInfo(clusterName, bulkloadId, ranges, resultStr, &errorMsg)) {
        BS_LOG(WARN, "get bulkload info failed, bulkload id [%s]", bulkloadId.c_str());
        recoverGenerationTask(snapshot);
        return false;
    }
    if (!commitTaskStatus(snapshot)) {
        BS_LOG(WARN,
               "get bulkoad info failed, bulkload id [%s] : "
               "serialize generation status failed!",
               bulkloadId.c_str());
        recoverGenerationTask(snapshot);
        return false;
    }
    return true;
}

bool GenerationKeeper::bulkload(const std::string& clusterName, const std::string& bulkloadId,
                                const ::google::protobuf::RepeatedPtrField<proto::ExternalFiles>& externalFiles,
                                const std::string& options, const std::string& action, std::string* errorMsg)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);

    auto snapshot = createTaskSnapshot();
    if (!_generationTask->bulkload(clusterName, bulkloadId, externalFiles, options, action, errorMsg)) {
        BS_LOG(WARN, "bulkload failed, bulkload id [%s]", bulkloadId.c_str());
        recoverGenerationTask(snapshot);
        return false;
    }
    if (!commitTaskStatus(snapshot)) {
        BS_LOG(WARN,
               "bulkoad failed, bulkload id [%s] : "
               "serialize generation status failed!",
               bulkloadId.c_str());
        recoverGenerationTask(snapshot);
        return false;
    }
    return true;
}

bool GenerationKeeper::rollBack(const string& clusterName, versionid_t versionId, int64_t startTimestamp,
                                string& errorMsg)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);

    auto snapshot = createTaskSnapshot();

    if (!_generationTask->rollBack(clusterName, _generationDir, versionId, startTimestamp, errorMsg)) {
        BS_LOG(WARN, "rollback to version[%d] failed", versionId);
        recoverGenerationTask(snapshot);
        return false;
    }

    if (!commitTaskStatus(snapshot)) {
        BS_LOG(WARN,
               "rollback to version[%d] failed : "
               "serialize generation status failed!",
               versionId);
        recoverGenerationTask(snapshot);
        return false;
    }
    return true;
}

bool GenerationKeeper::rollBackCheckpoint(const string& clusterName, checkpointid_t checkpointId,
                                          const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges,
                                          string& errorMsg)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);

    auto snapshot = createTaskSnapshot();

    if (!_generationTask->rollBackCheckpoint(clusterName, _generationDir, checkpointId, ranges, errorMsg)) {
        BS_LOG(WARN, "rollback to checkpoint[%ld] failed", checkpointId);
        recoverGenerationTask(snapshot);
        return false;
    }

    if (!commitTaskStatus(snapshot)) {
        BS_LOG(WARN,
               "rollback to checkpoint[%ld] failed : "
               "serialize generation status failed!",
               checkpointId);
        recoverGenerationTask(snapshot);
        return false;
    }
    return true;
}

bool GenerationKeeper::createSavepoint(const string& clusterName, checkpointid_t checkpointId,
                                       const std::string& comment, std::string& savepointStr, std::string& errorMsg)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    auto snapshot = createTaskSnapshot();
    if (!_generationTask->createSavepoint(clusterName, checkpointId, comment, savepointStr, errorMsg)) {
        BS_LOG(WARN, "createSavepoint for checkpoint[%ld] failed, clusterName[%s]", checkpointId, clusterName.c_str());
        recoverGenerationTask(snapshot);
        return false;
    }

    if (!commitTaskStatus(snapshot)) {
        BS_LOG(WARN,
               "createSavepoint for checkpoint[%ld] failed : "
               "serialize generation status failed!",
               checkpointId);
        recoverGenerationTask(snapshot);
        return false;
    }
    return true;
}

bool GenerationKeeper::removeSavepoint(const string& clusterName, checkpointid_t checkpointId, std::string& errorMsg)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    auto snapshot = createTaskSnapshot();
    if (!_generationTask->removeSavepoint(clusterName, checkpointId, errorMsg)) {
        BS_LOG(WARN, "removeSavepoint for checkpoint[%ld] failed", checkpointId);
        recoverGenerationTask(snapshot);
        return false;
    }

    if (!commitTaskStatus(snapshot)) {
        BS_LOG(WARN,
               "removeSavepoint for checkpoint[%ld] failed : "
               "serialize generation status failed!",
               checkpointId);
        recoverGenerationTask(snapshot);
        return false;
    }
    return true;
}

bool GenerationKeeper::getCheckpoint(const std::string& clusterName, checkpointid_t checkpointId,
                                     std::string* result) const
{
    ScopedLock lock(_decisionMutex);
    return _generationTask->getCheckpoint(clusterName, checkpointId, result);
}

bool GenerationKeeper::listCheckpoint(const std::string& clusterName, bool savepointOnly, uint32_t offset,
                                      uint32_t limit, std::string* result) const
{
    ScopedLock lock(_decisionMutex);
    return _generationTask->listCheckpoint(clusterName, savepointOnly, offset, limit, result);
}

bool GenerationKeeper::commitVersion(const std::string& clusterName, const proto::Range& range,
                                     const std::string& versionMeta, bool instantPublish, std::string& errorMsg)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    auto snapshot = createTaskSnapshot();
    if (!_generationTask->commitVersion(clusterName, range, versionMeta, instantPublish, errorMsg)) {
        BS_LOG(WARN, "commitVersion failed, clusterName[%s]", clusterName.c_str());
        recoverGenerationTask(snapshot);
        return false;
    }

    if (!commitTaskStatus(snapshot)) {
        errorMsg = "serialize generation status failed!";
        BS_LOG(WARN, "commitVersion for versionMeta[%s] failed : clusterName[%s], errMsg[%s]", versionMeta.c_str(),
               clusterName.c_str(), errorMsg.c_str());
        recoverGenerationTask(snapshot);
        return false;
    }
    return true;
}

bool GenerationKeeper::getCommittedVersions(const std::string& clusterName, const proto::Range& range, uint32_t limit,
                                            std::string& result, std::string& errorMsg)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    return _generationTask->getCommittedVersions(clusterName, range, limit, result, errorMsg);
}

bool GenerationKeeper::getReservedVersions(const std::string& clusterName, const proto::Range& range,
                                           std::set<indexlibv2::framework::VersionCoord>* reservedVersions) const
{
    ScopedLock lock(_decisionMutex);
    return _generationTask->getReservedVersions(clusterName, range, reservedVersions);
}

bool GenerationKeeper::markCheckpoint(const string& clusterName, versionid_t versionId, bool reserveFlag,
                                      string& errorMsg)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    auto snapshot = createTaskSnapshot();

    if (!_generationTask->markCheckpoint(clusterName, versionId, reserveFlag, errorMsg)) {
        BS_LOG(WARN, "createSnapshot for version[%d] failed", versionId);
        recoverGenerationTask(snapshot);
        return false;
    }

    if (!commitTaskStatus(snapshot)) {
        BS_LOG(WARN,
               "createSnapshot for version[%d] failed : "
               "serialize generation status failed!",
               versionId);
        recoverGenerationTask(snapshot);
        return false;
    }
    return true;
}

bool GenerationKeeper::updateConfig(const string& configPath)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    auto snapshot = createTaskSnapshot();

    if (!_generationTask->updateConfig(configPath)) {
        std::vector<ErrorInfo> errorInfos;
        _generationTask->transferErrorInfos(errorInfos);
        addErrorInfos(errorInfos);
        recoverGenerationTask(snapshot);
        return false;
    }
    unique_ptr<AppPlanMaker> maker(createAppPlanMaker(configPath, _generationTask->getHeartbeatType()));
    if (!maker.get()) {
        string errorMsg = "updateConfig failed: createAppPlanMaker failed!";
        REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
        recoverGenerationTask(snapshot);
        return false;
    }

    if (!commitTaskStatus(snapshot)) {
        string errorMsg = "updateConfig failed: serialize generation status failed!";
        REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
        recoverGenerationTask(snapshot);
        return false;
    }

    // commit to member with only memory operation.
    // make sure it's atomic
    maker->prepare(_workerTable);
    setAppPlanMaker(maker.release());
    resetAgentRolePlanMaker();
    _configPathChanged = true;
    return true;
}

const std::string GenerationKeeper::getConfigPath() const
{
    ScopedLock lock(_decisionMutex);
    return _generationTask->getConfigPath();
}

const std::string GenerationKeeper::getFatalErrorString() const
{
    ScopedLock lock(_generationMutex);
    return (_generationTask != nullptr) ? _generationTask->getFatalErrorString() : std::string("");
}

std::shared_ptr<GenerationTaskBase::AgentRoleTargetMap> GenerationKeeper::getAgentRoleTargetMap() const
{
    ScopedLock lock(_generationMutex);
    return (_generationTask != nullptr) ? _generationTask->getAgentRoleTargetMap() : nullptr;
}

void GenerationKeeper::setAgentRoleTargetMap(const std::shared_ptr<AgentRoleTargetMap>& targetMap)
{
    ScopedLock lock(_generationMutex);
    _generationTask->setAgentRoleTargetMap(targetMap);
}

void GenerationKeeper::stepBuild(proto::InformResponse* response)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    {
        ScopedLock lock(_decisionMutex);
        string debugMode = _generationTask->getDebugMode();
        if (debugMode != "step") {
            SET_ERROR_AND_RETURN(_generationTask->getBuildId(), response, ADMIN_INTERNAL_ERROR,
                                 "step build failed : [%s] debug mode is [%s].",
                                 _generationTask->getBuildId().ShortDebugString().c_str(), debugMode.c_str());
        }
    }

    workThread();
}

bool GenerationKeeper::createVersion(const string& clusterName, const string& mergeConfigName)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    auto snapshot = createTaskSnapshot();

    if (!_generationTask->createVersion(clusterName, mergeConfigName)) {
        recoverGenerationTask(snapshot);
        return false;
    }

    if (!commitTaskStatus(snapshot)) {
        string errorMsg = "createVersion failed: serialize generation status failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        recoverGenerationTask(snapshot);
        return false;
    }
    return true;
}

bool GenerationKeeper::switchBuild()
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    auto snapshot = createTaskSnapshot();

    if (!_generationTask->switchBuild()) {
        recoverGenerationTask(snapshot);
        return false;
    }

    if (!commitTaskStatus(snapshot)) {
        string errorMsg = "switchBuild failed: serialize generation status failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        recoverGenerationTask(snapshot);
        return false;
    }
    return true;
}

void GenerationKeeper::fillTaskInfo(int64_t taskId, TaskInfo* taskInfo, bool& exist)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    _generationTask->fillTaskInfo(taskId, *_workerTable, taskInfo, exist);
}

size_t GenerationKeeper::getRunningTaskNum() const
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    return _generationTask->getRunningTaskNum();
}

void GenerationKeeper::fillGenerationInfo(GenerationInfo* generationInfo, bool fillTaskHistory, bool fillCounterInfo,
                                          bool fillFlowInfo, bool fillEffectiveIndexInfo) const
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);

    for (auto& kv : _roleReleaseInfoMap) {
        auto roleReleaseInfo = generationInfo->mutable_rolereleaseinfos()->Add();
        roleReleaseInfo->set_rolename(kv.first);
        roleReleaseInfo->set_reachreleaselimit(kv.second.reachReleaseLimit);
        for (const auto& ip : kv.second.releasedIps) {
            roleReleaseInfo->add_releaseips(ip);
        }
    }

    if (_jobCounterCollector && fillCounterInfo) {
        CounterCollector::RoleCounterMap roleCounterMap;
        int64_t lastSyncTs = (int64_t)_jobCounterCollector->collect(roleCounterMap);
        int64_t lastVersionTs = _generationTask->getMaxLastVersionTimestamp();
        if (lastVersionTs >= lastSyncTs) {
            roleCounterMap.clear();
            if (_jobCounterCollector->syncCounters(true)) {
                lastSyncTs = _jobCounterCollector->collect(roleCounterMap);
                if (lastSyncTs < lastVersionTs) {
                    BS_LOG(ERROR, "[%s] force sync counter failed", _buildId.ShortDebugString().c_str());
                }
            }
        }

        for (auto& kv : roleCounterMap) {
            *(generationInfo->mutable_rolecounterinfo()->Add()) = kv.second;
        }
        _generationTask->fillGenerationInfo(generationInfo, _workerTable, fillTaskHistory, fillFlowInfo,
                                            fillEffectiveIndexInfo, roleCounterMap);
    } else {
        _generationTask->fillGenerationInfo(generationInfo, _workerTable, fillTaskHistory, fillFlowInfo,
                                            fillEffectiveIndexInfo, {});
    }
    _generationTask->fillAgentRoleTargetInfo(generationInfo);
}

void GenerationKeeper::fillSlowWorkerSlots(RoleSlotInfos& roleSlots)
{
    RoleSlotInfosPtr slowRoleSlotsPtr;
    _slowRoleSlotInfoGuard.get(slowRoleSlotsPtr);

    if (!slowRoleSlotsPtr) {
        return;
    }

    for (const auto& kv : *slowRoleSlotsPtr) {
        roleSlots.insert(kv);
    }
}

void GenerationKeeper::updateActiveNodeStatistics(const RoleSlotInfosPtr& assignRoleSlots,
                                                  RoleSlotInfosPtr& slowRoleSlots,
                                                  GenerationMetricsPtr& generationMetrics)
{
    if (!assignRoleSlots || !slowRoleSlots) {
        return;
    }
    struct NodeInfoUpdator {
        NodeInfoUpdator(const RoleSlotInfosPtr& assignRoleSlots, RoleSlotInfosPtr& slowRoleSlots,
                        RoleReleaseInfoMap& roleReleaseInfoMap, ProhibitedIpCollectorPtr& prohibitIpCollector,
                        GenerationMetricsPtr& generationMetrics)
            : mAssignRoleSlots(assignRoleSlots)
            , mSlowRoleSlots(slowRoleSlots)
            , mRoleReleaseInfoMap(roleReleaseInfoMap)
            , mProhibitIpCollector(prohibitIpCollector)
            , mGenerationMetrics(generationMetrics)
        {
        }
        const RoleSlotInfosPtr& mAssignRoleSlots;
        RoleSlotInfosPtr& mSlowRoleSlots;
        RoleReleaseInfoMap& mRoleReleaseInfoMap;
        ProhibitedIpCollectorPtr& mProhibitIpCollector;
        GenerationMetricsPtr& mGenerationMetrics;
        bool operator()(WorkerNodeBasePtr node)
        {
            string roleName = RoleNameGenerator::generateRoleName(node->getPartitionId());
            WorkerNodeBase::PBSlotInfos toReleaseSlotPBs;
            SlotInfos toReleaseSlotInfos;
            mGenerationMetrics->activeNodeCount += 1;
            auto it = mAssignRoleSlots->find(roleName);
            if (it != mAssignRoleSlots->end()) {
                mGenerationMetrics->assignSlotCount += 1;
                const SlotInfos& slots = it->second;
                for (const auto& slot : slots) {
                    auto resource = slot.slotResource.find("cpu");
                    if (resource) {
                        mGenerationMetrics->totalCpuAmount += resource->amount;
                    }
                    resource = slot.slotResource.find("mem");
                    if (resource) {
                        mGenerationMetrics->totalMemAmount += resource->amount;
                    }
                }
            } else {
                mGenerationMetrics->waitSlotNodeCount += 1;
            }

            if (node->getToReleaseSlots(toReleaseSlotPBs)) {
                toReleaseSlotInfos.reserve(toReleaseSlotPBs.size());
                for (const auto& slotPB : toReleaseSlotPBs) {
                    hippo::SlotInfo slotInfo;
                    slotInfo.role = roleName;
                    slotInfo.slotId.slaveAddress = slotPB.id().slaveaddress();
                    slotInfo.slotId.id = slotPB.id().id();
                    slotInfo.slotId.declareTime = slotPB.id().declaretime();
                    toReleaseSlotInfos.push_back(slotInfo);
                }
                mSlowRoleSlots->insert(make_pair(roleName, toReleaseSlotInfos));
                if (mRoleReleaseInfoMap.find(roleName) == mRoleReleaseInfoMap.end()) {
                    mRoleReleaseInfoMap[roleName] = RoleReleaseInfo();
                }
                if (node->exceedReleaseLimit()) {
                    mRoleReleaseInfoMap[roleName].reachReleaseLimit = true;
                }
                auto& releasedIps = mRoleReleaseInfoMap[roleName].releasedIps;
                for (const auto& slotinfo : toReleaseSlotInfos) {
                    bool existFlag = false;
                    for (const auto& ip : releasedIps) {
                        if (slotinfo.slotId.slaveAddress == ip) {
                            existFlag = true;
                            break;
                        }
                    }
                    if (existFlag == false) {
                        releasedIps.push_back(slotinfo.slotId.slaveAddress);
                        if (mProhibitIpCollector) {
                            mProhibitIpCollector->collect(slotinfo.slotId.slaveAddress);
                        }
                        mGenerationMetrics->totalReleaseSlowSlotCount += 1;
                    }
                }
            }
            return true;
        }
    };
    NodeInfoUpdator updator(assignRoleSlots, slowRoleSlots, _roleReleaseInfoMap, _prohibitIpCollector,
                            generationMetrics);
    _workerTable->forEachActiveNode(updator);
}

void GenerationKeeper::fillGenerationSlotInfo(const RoleSlotInfosPtr& roleSlotsPtr) const
{
    if (!roleSlotsPtr) {
        return;
    }
    struct SetSlotInfoFunctor {
        SetSlotInfoFunctor(const RoleSlotInfosPtr& roleSlotInfosPtr) : _roleSlotInfos(roleSlotInfosPtr) {}
        ~SetSlotInfoFunctor() {}

        bool operator()(WorkerNodeBasePtr node)
        {
            auto roleName = RoleNameGenerator::generateRoleName(node->getPartitionId());
            RoleSlotInfos::const_iterator it = _roleSlotInfos->find(roleName);
            if (it == _roleSlotInfos->end()) {
                return true;
            }
            HippoProtoHelper::setSlotInfo(it->second, node.get());
            return true;
        }
        RoleSlotInfosPtr _roleSlotInfos;
    };
    SetSlotInfoFunctor setSlotInfo(roleSlotsPtr);
    _workerTable->forEachActiveNode(setSlotInfo);
}

GenerationKeeper::GenerationRolePlanPtr GenerationKeeper::generateRolePlan(AgentSimpleMasterScheduler* scheduler)
{
    assert(_generationTask);
    if (!_generationTask) {
        return GenerationRolePlanPtr();
    }
    std::shared_ptr<AppPlanMaker> planMaker;
    _appPlanMakerGuard.get(planMaker);

    assert(planMaker);
    if (!planMaker) {
        return GenerationRolePlanPtr();
    }
    AppPlanPtr appPlan = planMaker->makeAppPlan();
    if (!appPlan) {
        return GenerationRolePlanPtr();
    }
    for (auto it = appPlan->rolePlans.begin(); it != appPlan->rolePlans.end(); ++it) {
        rewriteRoleArguments(it->first, it->second);
    }
    GenerationRolePlanPtr rolePlan = make_shared<GenerationRolePlan>();
    rolePlan->appPlan = appPlan;

    std::shared_ptr<AgentRolePlanMaker> agentRolePlanMaker;
    _agentRolePlanMakerGuard.get(agentRolePlanMaker);
    assert(agentRolePlanMaker);
    auto [agentRolePlan, agentRoleTargetMap] = agentRolePlanMaker->makePlan(scheduler, appPlan);
    rolePlan->agentRolePlan = agentRolePlan;
    rolePlan->allowAssignToGlobalAgents = agentRolePlanMaker->allowAssignedToGlobalAgentNodes();
    rolePlan->targetGlobalAgentGroupName = agentRolePlanMaker->getTargetGlobalAgentGroupId();
    syncConfigPathForAgentPlan(rolePlan);
    setAgentRoleTargetMap(agentRoleTargetMap);
    if (agentRolePlan != nullptr) {
        AgentRolePlanMaker::AgentGroupScheduleInfo info;
        agentRolePlanMaker->getScheduleInfo(info);
        _reporter.reportMetric(info);
    }
    return rolePlan;
}

bool GenerationKeeper::isStopped() const { return _isStopped; }

bool GenerationKeeper::needAutoStop()
{
    if (isStopped() || isStopping()) {
        return false;
    }
    return _generationTask->fatalErrorAutoStop();
}

void GenerationKeeper::workThread()
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    _refreshTimestamp = TimeUtility::currentTimeInSeconds();
    if (_isStopped) {
        return;
    }
    if (_needSyncStatus) {
        auto snapshot = createTaskSnapshot();
        if (!commitTaskStatus(snapshot)) {
            string errorMsg = "write status to zookeeper failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            recoverGenerationTask(snapshot);
        }
        return;
    }
    if (_generationTask->isStopped()) {
        string statusFile = fslib::util::FileUtil::joinFilePath(_generationDir, PathDefine::ZK_GENERATION_STATUS_FILE);
        string stopFile =
            fslib::util::FileUtil::joinFilePath(_generationDir, PathDefine::ZK_GENERATION_STATUS_STOP_FILE);
        if (moveGenerationStatus(statusFile, stopFile)) {
            if (_jobCounterCollector) {
                _jobCounterCollector->syncCounters(true);
                _jobCounterCollector->disableSyncCounter();
            }
            if (_checkpointMetricReporter) {
                _checkpointMetricReporter->clearAllLogInfo();
            }
            if (_taskStatusMetricReporter) {
                _taskStatusMetricReporter->clearAllLogInfo();
            }
            _isStopped = true;
        }
        return;
    }
    if (needAutoStop()) {
        BS_LOG(ERROR, "auto stop generation[%s] with fatal error",
               _generationTask->getBuildId().ShortDebugString().c_str());

        string msg =
            "auto stop generation [" + _generationTask->getBuildId().ShortDebugString() + "] with fatal error.";
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, msg,
                      BuildIdWrapper::getEventTags(_generationTask->getBuildId()));
        stopBuild();
        return;
    }
    RoleSlotInfosPtr assignedSlotInfoMap;
    _roleSlotInfoGuard.get(assignedSlotInfoMap);
    fillGenerationSlotInfo(assignedSlotInfoMap);

    auto snapshot = createTaskSnapshot();
    GenerationTaskBase::GenerationStep lastGenerationStep;
    BuildStep lastBuildStep;
    getStatus(lastGenerationStep, lastBuildStep);
    _generationTask->makeDecision(*_workerTable);
    if (!commitTaskStatus(snapshot)) {
        string errorMsg = "write status to zookeepr failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        recoverGenerationTask(snapshot);
        return;
    }
    syncProgress(lastGenerationStep, lastBuildStep);
    _workerTable->syncNodesStatus();

    RoleSlotInfosPtr slowRoleSlots(new RoleSlotInfos);
    GenerationMetricsPtr generationMetrics(new GenerationMetrics());
    updateActiveNodeStatistics(assignedSlotInfoMap, slowRoleSlots, generationMetrics);
    _metricsGuard.set(generationMetrics);
    _slowRoleSlotInfoGuard.set(slowRoleSlots);

    if (_metricsReporter) {
        _metricsReporter->updateMetricsRecord(*generationMetrics);
    }

    std::shared_ptr<AppPlanMaker> planMaker;
    _appPlanMakerGuard.get(planMaker);
    if (planMaker) {
        if (_configPathChanged) {
            // avoid dirty configPath in cache generate stale app plan
            planMaker->cleanCache();
            _configPathChanged = false;
        }
        planMaker->prepare(_workerTable);
        if (_scheduleMetricReporter) {
            _scheduleMetricReporter->updateMetrics(planMaker->getCandidates(), assignedSlotInfoMap);
        }
    }
    if (_catalogVersionPublisher) {
        GenerationInfo generationInfo;
        _generationTask->fillGenerationInfo(&generationInfo, /*workerTable*/ nullptr, /*fillTaskHistory*/ false,
                                            /*fillFlowInfo*/ false, /*fillEffectiveIndexInfo*/ false,
                                            /*roleCounterMap*/ {});
        _catalogVersionPublisher->publish(generationInfo);
    }
}

void GenerationKeeper::reportMetrics()
{
    if (_metricsReporter) {
        _metricsReporter->reportMetrics();
    }
}

bool GenerationKeeper::writeStatus(const string& statusStr)
{
    string finalStatusStr;
    if (statusStr.size() < GENERATION_STATUS_SIZE_THRESHOLD) {
        finalStatusStr = statusStr;
    } else {
        ZlibCompressor compressor;
        compressor.compress(statusStr, finalStatusStr);
    }
    if (WorkerState::EC_FAIL == _zkStatus.write(finalStatusStr)) {
        string errorMsg = "update generationd status [" + statusStr + "] failed.";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    _latestSnapshot = statusStr;
    return true;
}

bool GenerationKeeper::readStatus(string& content)
{
    WorkerState::ErrorCode ec = _zkStatus.read(content);
    if (WorkerState::EC_FAIL == ec) {
        stringstream ss;
        ss << "read generation state failed, ec[" << int(ec) << "].";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool GenerationKeeper::cleanGenerationDir() const
{
    const string& statusFile =
        fslib::util::FileUtil::joinFilePath(_generationDir, PathDefine::ZK_GENERATION_STATUS_FILE);
    bool exist = false;
    if (!fslib::util::FileUtil::isExist(statusFile, exist) || exist) {
        BS_LOG(ERROR, "the generation dir [%s] to delete has status file!", _generationDir.c_str());
        return false;
    }
    BS_LOG(INFO, "try to clean generation dir %s.", _generationDir.c_str());
    if (!fslib::util::FileUtil::removeIfExist(_generationDir)) {
        string errorMsg = "clean generation base dir [" + _generationDir + "] failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    BS_LOG(INFO, "cleanGenerationDir success for buildId[%s] with generation dir[%s]",
           _buildId.ShortDebugString().c_str(), _generationDir.c_str());
    return true;
}

bool GenerationKeeper::moveGenerationStatus(const string& src, const string& dst)
{
    fslib::ErrorCode ec;
    ec = FileSystem::isExist(src);
    if (fslib::EC_TRUE != ec && fslib::EC_FALSE != ec) {
        stringstream ss;
        ss << "check [" << src << "] isexist failed, error[" << FileSystem::getErrorString(ec) << "]!";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (fslib::EC_FALSE == ec) {
        BS_LOG(INFO, "[%s] not exist", src.c_str());
        return true;
    }

    ec = FileSystem::isExist(dst);
    if (fslib::EC_TRUE != ec && fslib::EC_FALSE != ec) {
        stringstream ss;
        ss << "check [" << dst << "] isexist failed, error[" << FileSystem::getErrorString(ec) << "]!";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (fslib::EC_TRUE == ec) {
        ec = FileSystem::remove(dst);
        if (fslib::EC_OK != ec) {
            stringstream ss;
            ss << "remove [" << dst << "]  failed, error[" << FileSystem::getErrorString(ec) << "]!";
            string errorMsg = ss.str();
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    ec = FileSystem::rename(src, dst);
    if (fslib::EC_OK != ec) {
        stringstream ss;
        ss << "remove generation status file[" << src << "] failed, error[" << FileSystem::getErrorString(ec) << "]!";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

AppPlanMaker* GenerationKeeper::createAppPlanMaker(const string& configPath, const string& heartBeatType)
{
    AppPlanMaker* maker = new AppPlanMaker(_adminServiceName, heartBeatType);
    string appPlanConf = fslib::util::FileUtil::joinFilePath(configPath, HIPPO_FILE_NAME);
    if (!maker->loadConfig(appPlanConf)) {
        string errorMsg = "Invalid hippo config : [" + appPlanConf + "]";
        REPORT_ERROR(SERVICE_ADMIN_ERROR, errorMsg);
        delete maker;
        return NULL;
    }
    if (!_specifiedWorkerPkgList.empty()) {
        maker->resetWorkerPackageList(_specifiedWorkerPkgList);
    }
    return maker;
}

template <class ResponseType>
void GenerationKeeper::initFromStatus(ResponseType* response)
{
    AppPlanMaker* maker = createAppPlanMaker(_generationTask->getConfigPath(), _generationTask->getHeartbeatType());
    if (!maker) {
        SET_ERROR_AND_RETURN(_generationTask->getBuildId(), response, ADMIN_INTERNAL_ERROR,
                             "start generation[%s] from config failed by initAppPlanMaker",
                             _generationTask->getBuildId().ShortDebugString().c_str());
    }
    setAppPlanMaker(maker);
    resetAgentRolePlanMaker();

    if (!_workerTable->startHeartbeat(_generationTask->getHeartbeatType())) {
        SET_ERROR_AND_RETURN(_generationTask->getBuildId(), response, ADMIN_INTERNAL_ERROR,
                             "start [%s] heartbeat FAILED!", _generationTask->getBuildId().ShortDebugString().c_str());
    }
    if (_generationTask->getDebugMode() == "run") {
        _loopThreadPtr = LoopThread::createLoopThread(bind(&GenerationKeeper::workThread, this), 1000 * 1000);
    }
    if (_generationTask->getDebugMode() == "step") {
        workThread();
    }
}

void GenerationKeeper::setAppPlanMaker(AppPlanMaker* maker)
{
    std::shared_ptr<AppPlanMaker> planMaker(maker);
    _appPlanMakerGuard.set(planMaker);
}

const proto::BuildId& GenerationKeeper::getBuildId() const
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    return _generationTask->getBuildId();
}

bool GenerationKeeper::isIndexDeleted() const
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_generationMutex);
    return _generationTask->isIndexDeleted();
}

void GenerationKeeper::clearThreads()
{
    if (_loopThreadPtr) {
        _loopThreadPtr->stop();
        _loopThreadPtr.reset();
    }
    releaseInnerResource();
    _workerTable->stopHeartbeat();
}

bool GenerationKeeper::cleanVersions(const string& clusterName, versionid_t versionId)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    auto snapshot = createTaskSnapshot();
    if (!_generationTask->cleanReservedCheckpoints(clusterName, versionId)) {
        BS_LOG(WARN, "cleanVersions fail for cluster[%s], versionId[%d]", clusterName.c_str(), versionId);
        recoverGenerationTask(snapshot);
        return false;
    }

    if (!commitTaskStatus(snapshot)) {
        BS_LOG(WARN,
               "clean reserved version list before version[%d] failed : "
               "serialize generation status failed!",
               versionId);
        recoverGenerationTask(snapshot);
        return false;
    }
    return _generationTask->cleanVersions(clusterName, versionId);
}

bool GenerationKeeper::clearCounters()
{
    CounterCollectorPtr collector;
    if (_jobCounterCollector) {
        collector = _jobCounterCollector;
    } else {
        CounterCollector* ccTmp = createCounterCollector(_generationTask->getCounterConfig());
        if (ccTmp) {
            collector.reset(ccTmp);
        }
    }
    if (collector) {
        return collector->clearCounters();
    }
    return false;
}

bool GenerationKeeper::needSyncCounter() const
{
    auto param = autil::EnvUtil::getEnv(BS_ENV_FILL_COUNTER_INFO.c_str());
    if (param == "false") {
        return false;
    }
    return true;
}

bool GenerationKeeper::prepareCounterCollector(const CounterConfig& basicCounterConfig)
{
    if (!needSyncCounter()) {
        return true;
    }
    CounterCollector* collector = createCounterCollector(basicCounterConfig);
    if (collector) {
        collector->syncCounters(true);
        _jobCounterCollector.reset(collector);
        return true;
    }
    return false;
}

CounterCollector* GenerationKeeper::createCounterCollector(CounterConfig counterConfig) const
{
    CounterCollector* collector = nullptr;
    if (counterConfig.position == "zookeeper") {
        string counterDirPath = fslib::util::FileUtil::joinFilePath(_generationDir, COUNTER_DIR_NAME);
        bool dirExist = false;
        if (!fslib::util::FileUtil::isDir(counterDirPath, dirExist)) {
            BS_LOG(ERROR, "check [%s] isexist failed", counterDirPath.c_str());
            return nullptr;
        }

        if (!dirExist) {
            if (!fslib::util::FileUtil::mkDir(counterDirPath, false)) {
                BS_LOG(ERROR, "create counter dir [%s] failed", counterDirPath.c_str());
            }
        }
        // TODO pass generationDir
        counterConfig.params[COUNTER_PARAM_FILEPATH] = _zkRoot;
        collector = CounterCollector::create(counterConfig, _buildId);
    } else if (counterConfig.position == "redis") {
        counterConfig.params[COUNTER_PARAM_REDIS_KEY] =
            CounterSynchronizer::getCounterRedisKey(_adminServiceName, _buildId);
        collector = CounterCollector::create(counterConfig, _buildId);
    }
    return collector;
}

const string& GenerationKeeper::createTaskSnapshot()
{
    if (_latestSnapshot.empty()) {
        _latestSnapshot = ToJsonString(*_generationTask);
    }
    return _latestSnapshot;
}

bool GenerationKeeper::commitTaskStatus(const string& oldStatus)
{
    assert(oldStatus == _latestSnapshot);
    string statusString = ToJsonString(*_generationTask);
    if (statusString == oldStatus) {
        _needSyncStatus = false;
        return true;
    }
    if (writeStatus(statusString)) {
        _needSyncStatus = false;
        return true;
    }
    _needSyncStatus = true;
    return false;
}

void GenerationKeeper::getStatus(GenerationTaskBase::GenerationStep& generationStep, proto::BuildStep& buildStep)
{
    generationStep = _generationTask->getGenerationStep();
    buildStep = _generationTask->getBuildStep();

    if (generationStep == GenerationTaskBase::GENERATION_STARTING && _startingBuildTimestamp == 0) {
        _startingBuildTimestamp = TimeUtility::currentTime() / 1000;
    }

    if (generationStep == GenerationTaskBase::GENERATION_STOPPING && _stoppingBuildTimestamp == 0) {
        _stoppingBuildTimestamp = TimeUtility::currentTime() / 1000;
    }
}

void GenerationKeeper::syncProgress(GenerationTaskBase::GenerationStep lastGenerationStep, BuildStep lastBuildStep)
{
    if (!_metricsReporter || !_generationTask) {
        return;
    }

    if (lastBuildStep == proto::BUILD_STEP_FULL && _generationTask->getBuildStep() == proto::BUILD_STEP_INC) {
        _metricsReporter->reportFullBuildTime(TimeUtility::currentTimeInSeconds() -
                                              _generationTask->getStartTimeStamp());
    }

    if (lastGenerationStep == GenerationTaskBase::GENERATION_STARTING &&
        _generationTask->getGenerationStep() == GenerationTaskBase::GENERATION_STARTED) {
        _metricsReporter->reportPrepareGenerationLatency(TimeUtility::currentTime() / 1000 - _startingBuildTimestamp);
    }

    if (lastGenerationStep == GenerationTaskBase::GENERATION_STOPPING &&
        _generationTask->getGenerationStep() == GenerationTaskBase::GENERATION_STOPPED) {
        _metricsReporter->reportStopGenerationLatency(TimeUtility::currentTime() / 1000 - _stoppingBuildTimestamp);
    }
}

int64_t GenerationKeeper::getStopTimeStamp() const
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_generationMutex);
    return _generationTask->getStopTimeStamp();
}

bool GenerationKeeper::isStarting() const
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_generationMutex);
    if (!_generationTask) {
        return false;
    }
    return _generationTask->isStarting();
}

bool GenerationKeeper::isStopping() const
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_generationMutex);
    if (!_generationTask) {
        return false;
    }
    return _generationTask->isStopping();
}

bool GenerationKeeper::getBuildStep(proto::BuildStep& buildStep) const
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_generationMutex);
    if (!_generationTask) {
        return false;
    }
    buildStep = _generationTask->getBuildStep();
    return true;
}

GenerationTaskBase::GenerationStep GenerationKeeper::getGenerationStep() const
{
    ScopedLock lock(_generationMutex);
    return _generationTask != nullptr ? _generationTask->getGenerationStep() : GenerationTaskBase::GENERATION_UNKNOWN;
}

int64_t GenerationKeeper::getLastRefreshTime() const { return _refreshTimestamp; }

void GenerationKeeper::updateLastRefreshTime() { _refreshTimestamp = TimeUtility::currentTimeInSeconds(); }

void GenerationKeeper::releaseInnerResource()
{
    _roleSlotInfoGuard.reset();
    _slowRoleSlotInfoGuard.reset();
    _appPlanMakerGuard.reset();
    _agentRolePlanMakerGuard.reset();
}

bool GenerationKeeper::getTotalRemainIndexSize(int64_t& totalSize)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_generationMutex);
    if (!_generationTask) {
        return false;
    }
    return _generationTask->getTotalRemainIndexSize(totalSize);
}

const std::string& GenerationKeeper::getIndexRoot() const { return _generationTask->getIndexRoot(); }

bool GenerationKeeper::initGraphParameter(const proto::CallGraphRequest* request, KeyValueMap& kvMap) const
{
#define CONVERT_TO_KVMAP(content, kvMap)                                                                               \
    try {                                                                                                              \
        auto any = ParseJson(content);                                                                                 \
        JsonMap jsonMap = AnyCast<JsonMap>(any);                                                                       \
        for (auto& it : jsonMap) {                                                                                     \
            kvMap[it.first] = AnyCast<string>(it.second);                                                              \
        }                                                                                                              \
    } catch (const ExceptionBase& e) {                                                                                 \
        BS_LOG(ERROR, "convert fail, exception [%s]", e.what());                                                       \
        return false;                                                                                                  \
    } catch (const exception& e) {                                                                                     \
        BS_LOG(ERROR, "convert fail, exception[%s]", e.what());                                                        \
        return false;                                                                                                  \
    } catch (...) {                                                                                                    \
        BS_LOG(ERROR, "unknown exception");                                                                            \
        return false;                                                                                                  \
    }

    kvMap["buildId"] = ProtoUtil::buildIdToStr(_buildId);
    string content;
    if (request->has_jsonparameterfilepath()) {
        string filePath = request->jsonparameterfilepath();
        if (FileSystem::getFsType(filePath) == FSLIB_FS_LOCAL_FILESYSTEM_NAME) {
            filePath = fslib::util::FileUtil::joinFilePath(_generationTask->getConfigPath(), filePath);
        }
        if (FileSystem::isExist(filePath) != fslib::EC_TRUE) {
            BS_LOG(ERROR, "parameter file [%s] not exist!", filePath.c_str());
            return false;
        }
        if (!fslib::util::FileUtil::readFile(filePath, content)) {
            BS_LOG(ERROR, "read parameter file [%s] fail!", filePath.c_str());
            return false;
        }
        if (!content.empty()) {
            CONVERT_TO_KVMAP(content, kvMap);
        }
    }
    if (request->has_jsonparameterstring()) {
        content = request->jsonparameterstring();
        if (!content.empty()) {
            CONVERT_TO_KVMAP(content, kvMap);
        }
    }
#undef CONVERT_TO_KVMAP
    return true;
}

vector<string> GenerationKeeper::getClusterNames() const { return _generationTask->getClusterNames(); }

void GenerationKeeper::resetAgentRolePlanMaker()
{
    AgentGroupConfigPtr config(new AgentGroupConfig);
    auto resourceReader = _generationTask->GetLatestResourceReader();
    bool isGeneralTask = (getGenerationTaskType() == GenerationTaskBase::TT_GENERAL);
    if (!resourceReader || !resourceReader->getAgentGroupConfig(isGeneralTask, *config)) {
        config.reset();
    }

    std::shared_ptr<AppPlanMaker> planMaker;
    _appPlanMakerGuard.get(planMaker);
    assert(planMaker);

    std::shared_ptr<AgentRolePlanMaker> maker = std::make_shared<AgentRolePlanMaker>(
        _buildId, _zkRoot, _adminServiceName, _amonitorPort, false, [this]() { return getAgentRoleTargetMap(); });

    maker->init(config, planMaker);
    _agentRolePlanMakerGuard.set(maker);
}

void GenerationKeeper::rewriteRoleArguments(const std::string& roleName, RolePlan& rolePlan)
{
    assert(rolePlan.processInfos.size() >= 1);
    assert(rolePlan.processInfos[0].cmd == "build_service_worker");
    rolePlan.processInfos[0].args.push_back(make_pair("-w", "${HIPPO_PROC_WORKDIR}"));
    rolePlan.processInfos[0].args.push_back(make_pair("-s", _buildId.appname()));
    rolePlan.processInfos[0].args.push_back(make_pair("-m", _adminServiceName));
    rolePlan.processInfos[0].args.push_back(make_pair("-z", _zkRoot));
    rolePlan.processInfos[0].args.push_back(make_pair("-r", roleName));
    rolePlan.processInfos[0].args.push_back(make_pair("-a", _amonitorPort));
    rolePlan.properties["useSpecifiedRestartCountLimit"] = "yes";
}

void GenerationKeeper::fillWorkerNodeStatus(const std::set<std::string>& roleNames,
                                            proto::WorkerNodeStatusResponse* response)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);

    struct FillNodeStatusFunctor {
        FillNodeStatusFunctor(const set<string>& targetRoles, set<string>& matchRoles,
                              proto::WorkerNodeStatusResponse* response)
            : _targetRoles(targetRoles)
            , _matchRoles(matchRoles)
            , _response(response)
        {
        }

        const set<string>& _targetRoles;
        set<string>& _matchRoles;
        proto::WorkerNodeStatusResponse* _response;

        bool operator()(WorkerNodeBasePtr node)
        {
            const proto::PartitionId& pid = node->getPartitionId();
            string roleName = RoleNameGenerator::generateRoleName(pid);
            if (_targetRoles.find(roleName) != _targetRoles.end()) {
                proto::WorkerNodeStatus* status = _response->add_nodestatus();
                status->set_rolename(roleName);
                *status->mutable_pid() = pid;
                status->set_targetstatus(node->getTargetStatusJsonStr());
                status->set_currentstatus(node->getCurrentStatusJsonStr());
                proto::WorkerNodeBase::PBSlotInfos pbSlotInfos;
                node->getSlotInfo(pbSlotInfos);
                status->set_slotinfos(HippoProtoHelper::getSlotInfoJsonString(pbSlotInfos));
                _matchRoles.insert(roleName);
            }
            return true;
        }
    };
    set<string> matchRoles;
    FillNodeStatusFunctor fillNodes(roleNames, matchRoles, response);
    _workerTable->forEachActiveNode(fillNodes);
    if (matchRoles.size() != roleNames.size()) {
        vector<string> nonExistRoles;
        for (auto& role : roleNames) {
            if (matchRoles.find(role) == matchRoles.end()) {
                nonExistRoles.push_back(role);
            }
        }
        std::string errorMsg = "non-existent roles:";
        errorMsg += autil::legacy::ToJsonString(nonExistRoles, true);
        response->add_errormessage(errorMsg.c_str());
    }
}

void GenerationKeeper::migrateTargetRoles(const set<string>& roleNames, map<string, string>* successRoleInfos)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);

    struct MigrateNodeFunctor {
        MigrateNodeFunctor(const set<string>& targetRoles, map<string, string>& successRoleInfos)
            : _targetRoles(targetRoles)
            , _successRoleInfos(successRoleInfos)
        {
        }

        const set<string>& _targetRoles;
        map<string, string>& _successRoleInfos;

        bool operator()(WorkerNodeBasePtr node)
        {
            string roleName = RoleNameGenerator::generateRoleName(node->getPartitionId());
            if (_targetRoles.find(roleName) != _targetRoles.end()) {
                node->changeSlots();
                WorkerNodeBase::PBSlotInfos pbSlotInfos;
                if (!node->getToReleaseSlots(pbSlotInfos)) {
                    BS_LOG(WARN,
                           "Find target role [%s] to migrate,"
                           " but no valid to-release-slots.",
                           roleName.c_str());
                } else {
                    assert(!pbSlotInfos.empty());
                    string targetSlotIps = pbSlotInfos.Get(0).id().slaveaddress();
                    for (size_t i = 1; i < pbSlotInfos.size(); i++) {
                        targetSlotIps += "#";
                        targetSlotIps += pbSlotInfos.Get(i).id().slaveaddress();
                    }
                    BS_LOG(INFO, "Find target role [%s] to migrate, trigger change slots [%s].", roleName.c_str(),
                           targetSlotIps.c_str());
                    _successRoleInfos[roleName] = targetSlotIps;
                }
            }
            return true;
        }
    };
    MigrateNodeFunctor migrateNodes(roleNames, *successRoleInfos);
    _workerTable->forEachActiveNode(migrateNodes);
}

void GenerationKeeper::fillWorkerRoleInfo(WorkerRoleInfoMap& roleInfoMap, bool onlyActive)
{
    FSLIB_LONG_INTERVAL_LOG("generation %s", _buildId.ShortDebugString().c_str());
    ScopedLock lock(_decisionMutex);
    auto workNodes = onlyActive ? _workerTable->getActiveNodes() : _workerTable->getAllNodes();
    for (auto& node : workNodes) {
        if (!node) {
            continue;
        }
        std::string roleName;
        if (!proto::ProtoUtil::partitionIdToRoleId(node->getPartitionId(), roleName)) {
            BS_LOG(ERROR, "convert partitionIdToRoleId fail, pid [%s]",
                   node->getPartitionId().ShortDebugString().c_str());
            continue;
        }
        // fill detail role info from worker node
        WorkerRoleInfo& info = roleInfoMap[roleName];
        info.identifier = node->getCurrentIdentifier();
        info.roleType = proto::ProtoUtil::toRoleString(node->getRoleType());
        info.heartbeatTime = node->getHeartbeatUpdateTime();
        info.workerStartTime = node->getWorkerStartTime();
        info.workerStatus = node->getWorkerStatus();
        info.isFinish = node->isFinished();
        info.taskIdentifier = node->getTaskIdentifierStr();
        node->getResourceAmount(info.cpuAmount, info.memAmount);
    }
    std::shared_ptr<AgentRoleTargetMap> roleTargetMap = getAgentRoleTargetMap();
    if (roleTargetMap) {
        // update generation level agent info
        for (auto it : *roleTargetMap) {
            auto& agentRole = it.first;
            auto& agentIdentifier = it.second.first;
            for (auto& role : it.second.second) {
                auto roleIt = roleInfoMap.find(role);
                if (roleIt != roleInfoMap.end()) {
                    roleIt->second.agentRoleName = agentRole;
                    roleIt->second.agentIdentifier = agentIdentifier;
                }
            }
        }
    }
}

void GenerationKeeper::syncConfigPathForAgentPlan(const GenerationRolePlanPtr& rolePlan)
{
    if (!rolePlan) {
        return;
    }
    string configPath;
    {
        ScopedLock lock(_generationMutex);
        configPath = _generationTask->getConfigPath();
    }
    rolePlan->configPath = configPath;
    if (!rolePlan->agentRolePlan) {
        return;
    }
    for (auto iter = rolePlan->agentRolePlan->begin(); iter != rolePlan->agentRolePlan->end(); iter++) {
        iter->second->configPath = configPath;
    }
}

void GenerationKeeper::fillGenerationCheckpointInfo(
    std::map<std::string, std::vector<std::pair<std::string, int64_t>>>& infoMap)
{
    if (_checkpointMetricReporter) {
        _checkpointMetricReporter->fillCheckpointInfo(infoMap);
    }
}

void GenerationKeeper::fillGenerationTaskNodeStatus(std::vector<TaskStatusMetricReporter::TaskLogItem>& taskInfo)
{
    if (_taskStatusMetricReporter) {
        _taskStatusMetricReporter->fillTaskLogInfo(taskInfo);
    }
}

void GenerationKeeper::fillGenerationGraphData(
    std::string& flowGraph, std::map<std::string, std::string>& flowSubGraphMap,
    std::map<std::string, std::map<std::string, std::string>>& taskDetailInfoMap)
{
    ScopedLock lock(_decisionMutex);
    auto taskFlowManager = _generationTask->getTaskFlowManager();
    if (taskFlowManager) {
        flowGraph = taskFlowManager->getDotString(false);
        taskFlowManager->fillFlowSubGraphInfo(flowSubGraphMap, taskDetailInfoMap);
    }
}

std::string GenerationKeeper::getGenerationTaskTypeString() const
{
    auto type = getGenerationTaskType();
    switch (type) {
    case GenerationTaskBase::TT_SERVICE:
        return std::string("service");
    case GenerationTaskBase::TT_JOB:
        return std::string("job");
    case GenerationTaskBase::TT_CUSTOMIZED:
        return std::string("customized");
    case GenerationTaskBase::TT_GENERAL:
        return std::string("general_task");
    case GenerationTaskBase::TT_UNKNOWN:
        return std::string("unknown");
    default:
        assert(false);
    }
    return std::string("unknown");
}

}} // namespace build_service::admin
