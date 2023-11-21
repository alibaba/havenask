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
#include "build_service/admin/GraphGenerationTask.h"

#include <iosfwd>
#include <map>
#include <utility>

#include "alog/Logger.h"
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
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/BrokerTopicAccessor.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/GraphConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ProtoUtil.h"

using namespace std;
using namespace build_service::proto;
using namespace autil;
using namespace autil::legacy;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, GraphGenerationTask);

GraphGenerationTask::GraphGenerationTask(const proto::BuildId& buildId, cm_basic::ZkWrapper* zkWrapper)
    : GenerationTaskBase(buildId, TT_CUSTOMIZED, zkWrapper)
{
}

GraphGenerationTask::~GraphGenerationTask() {}

void GraphGenerationTask::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { GenerationTaskBase::Jsonize(json); }

bool GraphGenerationTask::restart(const std::string& statusString, const std::string& generationDir)
{
    if (!loadFromString(statusString, generationDir)) {
        return false;
    }
    _step = GENERATION_STARTING;
    BS_LOG(INFO, "generation [%s] restarted", _buildId.ShortDebugString().c_str());
    return true;
}

bool GraphGenerationTask::loadFromString(const std::string& statusString, const std::string& generationDir)
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
            BS_LOG(ERROR, "recover GraphGenerationTask from status failed, exception[%s]", e.what());
            BS_LOG(ERROR, "recover failed with status [%s]", statusString.c_str());
            return false;
        }
    }
    return true;
}

bool GraphGenerationTask::loadFromConfig(const string& configPath, const string& generationDir,
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

    string targetGraph;
    config::GraphConfig graphConfig;

    if (!resourceReader->getGraphConfig(graphConfig)) {
        REPORT(SERVICE_ERROR_CONFIG, "init [%s] graph failed, no graph config", _buildId.ShortDebugString().c_str());
        return false;
    }
    targetGraph = graphConfig.getGraphName();
    const KeyValueMap& params = graphConfig.getGraphParam();
    vector<string> graphParamKeys;
    auto iter = params.begin();
    for (; iter != params.end(); iter++) {
        kvMap[iter->first] = iter->second;
        graphParamKeys.push_back(iter->first);
    }
    kvMap["user_define_parameter_keys"] = ToJsonString(graphParamKeys, true);
    if (!_taskFlowManager->loadSubGraph("", targetGraph, kvMap)) {
        REPORT(SERVICE_ERROR_CONFIG, "init [%s] graph failed", _buildId.ShortDebugString().c_str());
        return false;
    }
    _taskFlowManager->stepRun();

    setConfigPath(configPath);
    _step = GENERATION_STARTING;
    _startTimeStamp = autil::TimeUtility::currentTimeInSeconds();
    _generationDir = generationDir;

    return true;
}

void GraphGenerationTask::makeDecision(WorkerTable& workerTable)
{
    _taskFlowManager->stepRun();
    // keep relative for default graph task
    _flowCleaner->cleanFlows(_taskFlowManager, _generationDir, true);
    switch (_step) {
    case GENERATION_STARTING: {
        common::BrokerTopicAccessorPtr brokerTopicAccessor;
        _resourceManager->getResource(brokerTopicAccessor);
        if (!brokerTopicAccessor->prepareBrokerTopics()) {
            return;
        }

        _step = GENERATION_STARTED;
        BS_LOG(INFO, "prepare generation[%s] end", _buildId.ShortDebugString().c_str());
        break;
    }
    case GENERATION_STARTED: {
        common::BrokerTopicAccessorPtr brokerTopicAccessor;
        _resourceManager->getResource(brokerTopicAccessor);
        brokerTopicAccessor->prepareBrokerTopics();
        brokerTopicAccessor->clearUselessBrokerTopics(false);
        _configCleaner->cleanObsoleteConfig();
        runTasks(workerTable);
        if (isAllFlowFinished()) {
            _step = GENERATION_STOPPING;
        }
        break;
    }
    case GENERATION_STOPPING: {
        common::BrokerTopicAccessorPtr brokerTopicAccessor;
        _resourceManager->getResource(brokerTopicAccessor);
        if (!brokerTopicAccessor->clearUselessBrokerTopics(true)) {
            return;
        }
        _step = GENERATION_STOPPED;
        BS_LOG(INFO, "stop generation[%s] end", _buildId.ShortDebugString().c_str());
        break;
    }
    case GENERATION_STOPPED:
    default:
        break;
    };
}

bool GraphGenerationTask::isAllFlowFinished()
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

void GraphGenerationTask::fillGenerationInfo(proto::GenerationInfo* generationInfo, const WorkerTable* workerTable,
                                             bool fillTaskHistory, bool fillFlowInfo, bool fillEffectiveIndexInfo,
                                             const CounterCollector::RoleCounterMap& roleCounterMap)
{
    generationInfo->set_configpath(getConfigPath());
    generationInfo->set_buildstep(stepToString(_step));
    if (fillFlowInfo && workerTable) {
        _taskFlowManager->fillTaskFlowInfos(generationInfo, *workerTable, roleCounterMap);
    }
}

bool GraphGenerationTask::updateConfig(const std::string& configPath)
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

}} // namespace build_service::admin
