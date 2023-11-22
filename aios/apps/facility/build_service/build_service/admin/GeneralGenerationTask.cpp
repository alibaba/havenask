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
#include "build_service/admin/GeneralGenerationTask.h"

#include <map>
#include <ostream>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "beeper/beeper.h"
#include "build_service/admin/ConfigCleaner.h"
#include "build_service/admin/FlowIdMaintainer.h"
#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/admin/controlflow/TaskFactory.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/GeneralTaskController.h"
#include "build_service/admin/taskcontroller/TaskMaintainer.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/WorkerNode.h"

using namespace std;
using namespace build_service::proto;
using namespace autil;
using namespace autil::legacy;

namespace build_service { namespace admin {

BS_LOG_SETUP(admin, GeneralGenerationTask);

GeneralGenerationTask::GeneralGenerationTask(const proto::BuildId& buildId, cm_basic::ZkWrapper* zkWrapper)
    : GenerationTaskBase(buildId, TT_GENERAL, zkWrapper)
{
}

GeneralGenerationTask::~GeneralGenerationTask() {}

void GeneralGenerationTask::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { GenerationTaskBase::Jsonize(json); }

bool GeneralGenerationTask::restart(const std::string& statusString, const std::string& generationDir)
{
    if (!loadFromString(statusString, generationDir)) {
        return false;
    }
    _step = GENERATION_STARTED;
    BS_LOG(INFO, "generation [%s] restarted", _buildId.ShortDebugString().c_str());
    return true;
}

bool GeneralGenerationTask::loadFromString(const std::string& statusString, const std::string& generationDir)
{
    try {
        FromJsonString(*this, statusString);
    } catch (const autil::legacy::ExceptionBase& e) {
        // log length maybe overflow and only limited content is showed
        // so we use two log record to show exception and status string both
        ZlibCompressor compressor;
        string decompressedStatus;
        if (!compressor.decompress(statusString, decompressedStatus)) {
            BS_LOG(ERROR, "decompress status string failed [%s]", statusString.c_str());
            return false;
        }
        try {
            FromJsonString(*this, decompressedStatus);
        } catch (const ExceptionBase& e) {
            BS_LOG(ERROR, "recover GeneralGenerationTask from status failed, exception[%s]", e.what());
            BS_LOG(ERROR, "recover failed with status [%s]", statusString.c_str());
            return false;
        }
    }
    return true;
}

bool GeneralGenerationTask::loadFromConfig(const string& configPath, const string& generationDir,
                                           const string& dataDescriptionKvs, BuildStep buildStep)
{
    config::ResourceReaderPtr resourceReader(new config::ResourceReader(configPath));
    resourceReader->init();
    config::ConfigReaderAccessorPtr configResource;
    _resourceManager->getResource(configResource);
    if (!configResource->addConfig(resourceReader, false)) {
        BS_LOG(ERROR, "GenerationTask addConfig failed, configPaht[%s]", configPath.c_str());
        return false;
    }

    KeyValueMap kvMap;
    kvMap["buildId"] = ProtoUtil::buildIdToStr(_buildId);
    kvMap["buildStep"] = buildStep == proto::BUILD_STEP_FULL ? config::BUILD_STEP_FULL_STR : config::BUILD_STEP_INC_STR;
    kvMap["configPath"] = configPath;

    string targetGraph = "Idle.graph";
    if (!_taskFlowManager->loadSubGraph("", targetGraph, kvMap)) {
        REPORT(SERVICE_ERROR_CONFIG, "init [%s] graph failed", _buildId.ShortDebugString().c_str());
        return false;
    }
    _taskFlowManager->stepRun();

    setConfigPath(configPath);
    _step = GENERATION_STARTED;
    _startTimeStamp = autil::TimeUtility::currentTimeInSeconds();
    _generationDir = generationDir;
    return true;
}

void GeneralGenerationTask::makeDecision(WorkerTable& workerTable)
{
    _taskFlowManager->stepRun();
    // keep relative for general generation task
    // keep recent for general generation task
    _flowCleaner->cleanFlows(_taskFlowManager, _generationDir, /*keepRelativeFlow*/ true, /*keepRecentFlow*/ true);
    switch (_step) {
    case GENERATION_STARTING: {
        _step = GENERATION_STARTED;
        BS_LOG(INFO, "prepare generation[%s] end", _buildId.ShortDebugString().c_str());
        break;
    }
    case GENERATION_STARTED: {
        _configCleaner->cleanObsoleteConfig();
        runTasks(workerTable);
        if (isAllFlowFinished()) {
            _step = GENERATION_STOPPING;
        }
        break;
    }
    case GENERATION_STOPPING: {
        workerTable.clearAllNodes();
        _step = GENERATION_STOPPED;
        BS_LOG(INFO, "stop generation[%s] end", _buildId.ShortDebugString().c_str());
        break;
    }
    case GENERATION_STOPPED:
    default:
        break;
    };
}

bool GeneralGenerationTask::isAllFlowFinished()
{
    auto allFlows = _taskFlowManager->getAllFlow();
    for (auto flow : allFlows) {
        auto status = flow->getStatus();
        if (status != TaskFlow::tf_finish && status != TaskFlow::tf_stopped) {
            return false;
        }
    }
    return true;
}

void GeneralGenerationTask::fillGenerationInfo(proto::GenerationInfo* generationInfo, const WorkerTable* workerTable,
                                               bool fillTaskHistory, bool fillFlowInfo, bool fillEffectiveIndexInfo,
                                               const CounterCollector::RoleCounterMap& roleCounterMap)
{
    generationInfo->set_configpath(getConfigPath());
    generationInfo->set_buildstep(stepToString(_step));
    if (fillFlowInfo && workerTable) {
        _taskFlowManager->fillTaskFlowInfos(generationInfo, *workerTable, roleCounterMap);
    }
}

bool GeneralGenerationTask::updateConfig(const std::string& configPath)
{
    if (isSuspending()) {
        REPORT(SERVICE_ERROR_NONE, "can't update config when generation is suspending.");
        return false;
    }

    config::ConfigReaderAccessorPtr configReaderAccessor;
    _resourceManager->getResource(configReaderAccessor);
    config::ResourceReaderPtr resourceReader(new config::ResourceReader(configPath));
    resourceReader->init();
    configReaderAccessor->addConfig(resourceReader, false);

    auto allTasks = _taskFlowManager->getAllTask();
    for (auto task : allTasks) {
        if (!task->isTaskFinish() && !task->isTaskStopped() && !task->isTaskStopping()) {
            if (!task->updateConfig()) {
                REPORT(SERVICE_ERROR_CONFIG, "task [%s:%s] update config failed", task->getTaskType().c_str(),
                       task->getIdentifier().c_str());
                return false;
            }
            string msg =
                "task [" + task->getTaskType() + ":" + task->getIdentifier() + "] update config [" + configPath + "]";
            BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, msg);
        }
    }
    setConfigPath(configPath);
    return true;
}

bool GeneralGenerationTask::doStartTask(int64_t taskId, const string& taskName, const string& taskConfigPath,
                                        const string& clusterName, const string& taskParam,
                                        GenerationTaskBase::StartTaskResponse* response)
{
    if (taskId < 0) {
        response->message =
            string("check taskId fail, taskId [") + StringUtil::toString(taskId) + "] should not less than 0 ";
        return false;
    }

    string taskIdStr = StringUtil::toString(taskId);
    auto taskMaintainer = getUserTask(taskId);
    if (taskMaintainer) {
        BS_LOG(WARN, "task [%ld] already exist", taskId);
        response->message = std::string("start task: ") + taskIdStr + " failed";
        response->duplicated = true;
        response->needRecover = false;
        return false;
    }
    auto allFlows = _taskFlowManager->getAllFlow();
    for (auto& flow : allFlows) {
        if (flow->getFlowId() == "0") {
            // flow id 0 is the default flow created with generation
            continue;
        }
        if (!flow->isFlowFinish() && !flow->isFlowStopped()) {
            BS_LOG(ERROR, "graph[%s]flow[%s] is running, start task [%ld] failed", flow->getGraphId().c_str(),
                   flow->getFlowId().c_str(), taskId);
            response->message = std::string("Flow ") + flow->getFlowId() + " is running";
            response->needRecover = false;
            return false;
        }
    }

    GeneralTaskParam param;
    try {
        autil::legacy::FromJsonString(param, taskParam);
    } catch (const autil::legacy::ExceptionBase& e) {
        std::stringstream ss;
        ss << "invalid general task param:" << taskParam;
        BS_LOG(ERROR, "%s", ss.str().c_str());
        return false;
    }

    // TODO(hanyao): read and validate cluster name from config

    KeyValueMap kvMap;
    kvMap["taskParam"] = taskParam;
    kvMap["taskConfigPath"] = taskConfigPath;
    kvMap["clusterName"] = clusterName;
    kvMap["buildId"] = ProtoUtil::buildIdToStr(_buildId);

    kvMap["taskId"] = taskIdStr;
    kvMap["kernalType"] = taskName;

    if (!_taskFlowManager->loadSubGraph("", "BuildInTask.graph", kvMap)) {
        response->message = string("start task: ") + taskIdStr + " failed";
        return false;
    }
    _taskFlowManager->stepRun();
    string taskTag = getUserTaskTag(taskId);
    vector<string> flowIds;
    _taskFlowManager->getFlowIdByTag(taskTag, flowIds);
    assert(flowIds.size() == 1);
    TaskFlowPtr taskFlow = _taskFlowManager->getFlow(flowIds[0]);
    if (taskFlow->hasFatalError()) {
        response->message = string("start task: ") + taskIdStr + " failed";
        _taskFlowManager->removeFlow(flowIds[0], true);
        return false;
    }
    taskMaintainer = _taskFlowManager->getTaskMaintainer(taskFlow);
    assert(taskMaintainer);
    proto::TaskInfo taskInfo;
    TaskNodes taskNodes;
    CounterCollector::RoleCounterMap counterMap;
    taskMaintainer->collectTaskInfo(&taskInfo, taskNodes, counterMap);
    response->message = taskInfo.taskinfo();
    return true;
}

bool GeneralGenerationTask::doStopTask(int64_t taskId, std::string& errorMsg)
{
    std::string taskFlowTag = getUserTaskTag(taskId);
    vector<string> flowIds;
    _taskFlowManager->getFlowIdByTag(taskFlowTag, flowIds);
    assert(flowIds.size() <= 1);
    if (flowIds.size() == 0) {
        return true;
    }
    auto flow = _taskFlowManager->getFlow(flowIds[0]);
    if (flow) {
        flow->stopFlow();
    }
    return true;
}

}} // namespace build_service::admin
