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
#include "build_service/admin/taskcontroller/ProcessorTaskWrapper.h"

using namespace std;
using namespace autil;
using namespace build_service::proto;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ProcessorTaskWrapper);

ProcessorTaskWrapper::ProcessorTaskWrapper(const std::string& clusterName, const ProcessorTaskPtr& processorTask,
                                           bool disableOptimize)
    : AdminTaskBase(processorTask->getBuildId(), processorTask->getTaskResourceManager())
    , _processorTask(processorTask)
    , _needReplaceProcessorTask(false)
    , _clusterName(clusterName)
    , _disableOptimize(disableOptimize)
{
    //_configPath = processorTask->getUsingConfig();
    _configPath = processorTask->getConfigInfo().configPath;
    processorTask->getSchemaId(clusterName, _schemaId);
    string clusterNames = StringUtil::toString(_processorTask->getClusterNames(), ",");
    BS_LOG(INFO, "new processor task wrapper, config path [%s], cluster name [%s], actual cluster names[%s]",
           _configPath.c_str(), clusterName.c_str(), clusterNames.c_str());
}

ProcessorTaskWrapper::~ProcessorTaskWrapper()
{
    string clusterNames = StringUtil::toString(_processorTask->getClusterNames(), ",");
    BS_LOG(INFO, "destroy processor task wrapper, config path [%s], cluster name [%s], actual cluster names[%s]",
           _configPath.c_str(), _clusterName.c_str(), clusterNames.c_str());
}

bool ProcessorTaskWrapper::updateConfig()
{
    if (_processorTask->checkAllClusterConfigEqual()) {
        if (_processorTask->updateConfig()) {
            _configPath = _processorTask->getConfigInfo().configPath;
            return true;
        }
        return false;
    }
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    auto reader = readerAccessor->getConfig(_clusterName, _schemaId);
    if (!reader) {
        return false;
    }

    _processorTask->suspendTask(false);
    string clusterNames = StringUtil::toString(_processorTask->getClusterNames(), ",");
    BS_LOG(INFO, "suspend old processor task[%s], config path [%s], cluster names[%s]", _clusterName.c_str(),
           _configPath.c_str(), clusterNames.c_str());
    _needReplaceProcessorTask = true;
    if (createProcessorTask(reader->getOriginalConfigPath())) {
        _configPath = reader->getOriginalConfigPath();
        return true;
    }
    return false;
}

ProcessorTaskPtr ProcessorTaskWrapper::createProcessorTask(const std::string& configPath)
{
    ProcessorTaskPtr processor(new ProcessorTask(_buildId, _processorTask->getBuildStep(), _resourceManager));
    vector<string> clusterNames;
    clusterNames.push_back(_clusterName);
    auto configInfo = _processorTask->getConfigInfo();
    configInfo.configPath = configPath;
    if (!processor->init(_processorTask->getInput(), _processorTask->getOutput(), clusterNames, configInfo,
                         _processorTask->isTablet())) {
        BS_LOG(ERROR, "init processor task failed");
        return ProcessorTaskPtr();
    }
    processor->startFromLocator(_processorTask->getLocator());
    return processor;
}

bool ProcessorTaskWrapper::resumeTask()
{
    auto clusterNames = _processorTask->getClusterNames();
    if (clusterNames.size() == 1) {
        AdminTaskBase::resumeTask();
        return _processorTask->resumeTask();
    }

    ProcessorTaskPtr processor = createProcessorTask(_configPath);
    if (!processor) {
        return false;
    }
    _processorTask = processor;
    _needReplaceProcessorTask = false;
    auto clusterNamesStr = StringUtil::toString(_processorTask->getClusterNames(), ",");
    BS_LOG(INFO, "resume replace new processor task[%s], config path [%s], cluster names[%s]", _clusterName.c_str(),
           _configPath.c_str(), clusterNamesStr.c_str());
    AdminTaskBase::resumeTask();
    return true;
}

bool ProcessorTaskWrapper::run(WorkerNodes& workerNodes)
{
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    auto reader = readerAccessor->getConfig(_configPath);
    if (!reader) {
        // when config deleted processor auto stopped
        workerNodes.clear();
        _processorTask->notifyStopped();
        _taskStatus = TASK_FINISHED;
        string clusterNames = StringUtil::toString(_processorTask->getClusterNames(), ",");
        BS_LOG(INFO,
               "no config path, "
               "processor task auto finished, "
               "config path [%s], cluster names[%s]",
               _configPath.c_str(), clusterNames.c_str());
        return true;
    }
    if (_needReplaceProcessorTask) {
        _processorTask->waitSuspended(workerNodes);
        auto status = _processorTask->getTaskStatus();
        if (status == TASK_STOPPED || status == TASK_SUSPENDED) {
            ProcessorTaskPtr processor = createProcessorTask(_configPath);
            if (processor) {
                _processorTask = processor;
                _needReplaceProcessorTask = false;
                string clusterNames = StringUtil::toString(_processorTask->getClusterNames(), ",");
                BS_LOG(INFO, "replace new processor task[%s], config path [%s], cluster names[%s]",
                       _clusterName.c_str(), _configPath.c_str(), clusterNames.c_str());
            }
        }
        return false;
    }

    ProcessorNodes processorNodes;
    processorNodes.reserve(workerNodes.size());
    for (auto& workerNode : workerNodes) {
        processorNodes.push_back(DYNAMIC_POINTER_CAST(ProcessorNode, workerNode));
    }
    bool ret = false;
    ret = _processorTask->run(processorNodes);
    workerNodes.clear();
    workerNodes.reserve(processorNodes.size());
    for (auto& processorNode : processorNodes) {
        workerNodes.push_back(processorNode);
    }
    if (workerNodes.size() > 0) {
        _hasCreateNodes = true;
    }

    auto status = _processorTask->getTaskStatus();
    if ((status == TASK_STOPPED && _taskStatus != TASK_STOPPED) ||
        (status == TASK_SUSPENDED && _taskStatus != TASK_SUSPENDED &&
         _processorTask->getSuspendReason() != "for_optimize")) {
        ProcessorTaskPtr processor = createProcessorTask(_configPath);
        if (processor) {
            _processorTask = processor;
            string clusterNames = StringUtil::toString(_processorTask->getClusterNames(), ",");
            BS_LOG(INFO, "replace new processor task[%s], config path [%s], cluster names[%s]", _clusterName.c_str(),
                   _configPath.c_str(), clusterNames.c_str());
        }
        return false;
    }

    if (ret) {
        _taskStatus = TASK_FINISHED;
    }
    return ret;
}

void ProcessorTaskWrapper::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("current_config_path", _configPath);
    json.Jsonize("current_cluster_name", _clusterName);
    json.Jsonize("current_schema_id", _schemaId);
    json.Jsonize("need_replace_current_processor", _needReplaceProcessorTask);
    json.Jsonize("processor_task", *_processorTask);
    json.Jsonize("disable_optimize", _disableOptimize, _disableOptimize);
    json.Jsonize("status", _taskStatus, _taskStatus);
    json.Jsonize("has_create_nodes", _hasCreateNodes, _hasCreateNodes);
}

string ProcessorTaskWrapper::getTaskPhaseIdentifier() const
{
    if (_processorTask) {
        return _processorTask->getTaskPhaseIdentifier();
    }
    return AdminTaskBase::getTaskPhaseIdentifier();
}

bool ProcessorTaskWrapper::getTaskRunningTime(int64_t& intervalInMicroSec) const
{
    if (_processorTask) {
        return _processorTask->getTaskRunningTime(intervalInMicroSec);
    }
    return AdminTaskBase::getTaskRunningTime(intervalInMicroSec);
}

void ProcessorTaskWrapper::doSupplementLableInfo(KeyValueMap& info) const
{
    info["current_cluster_name"] = _clusterName;
    info["current_schema_id"] = StringUtil::toString(_schemaId);
    info["disable_optimize"] = _disableOptimize ? "true" : "false";
    info["need_replace"] = _needReplaceProcessorTask ? "true" : "false";
    if (_processorTask) {
        _processorTask->doSupplementLableInfo(info);
    }
}

void ProcessorTaskWrapper::setBeeperTags(const beeper::EventTagsPtr beeperTags)
{
    assert(beeperTags);
    AdminTaskBase::setBeeperTags(beeperTags);
}

}} // namespace build_service::admin
