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
#include "build_service/worker/TaskStateHandler.h"

#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/CounterSynchronizerCreator.h"
#include "build_service/common/CpuSpeedEstimater.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/io/InputCreator.h"
#include "build_service/io/OutputCreator.h"
#include "build_service/plugin/Module.h"
#include "build_service/plugin/ModuleFactory.h"
#include "build_service/proto/TaskIdentifier.h"
#include "build_service/util/RangeUtil.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace autil;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::task_base;
using namespace indexlib::util;

namespace build_service { namespace worker {
BS_LOG_SETUP(worker, TaskStateHandler);

TaskStateHandler::TaskStateHandler(const PartitionId& pid, indexlib::util::MetricProviderPtr metricProvider,
                                   const LongAddress& address, const string& appZkRoot, const string& adminServiceName,
                                   const string& epochId)
    : WorkerStateHandler(pid, metricProvider, appZkRoot, adminServiceName, epochId)
    , _startTimestamp(-1)
    , _reusePluginManager(false)

{
    *_current.mutable_longaddress() = address;
}

TaskStateHandler::~TaskStateHandler()
{
    _task.reset();
    _plugInManager.reset();
    _resourceReader.reset();
}

bool TaskStateHandler::init()
{
    bool ret = true;
    ret = _cpuEstimater.Start(/*sampleCountLimit=*/5, /*checkInterval=*/60);
    if (!ret) {
        return false;
    }
    ret = _networkEstimater.Start();
    return ret;
}

TaskFactory* TaskStateHandler::getTaskFactory(const std::string& moduleName)
{
    if (moduleName.empty()) {
        return &_buildInTaskFactory;
    }
    auto module = _plugInManager->getModule(moduleName);
    if (!module) {
        string errorMsg = "Init Task Factory failed. no module name[" + moduleName + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return nullptr;
    }
    auto factory = dynamic_cast<TaskFactory*>(module->getModuleFactory());
    if (!factory) {
        string errorMsg = "Invalid module[" + moduleName + "].";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return nullptr;
    }
    return factory;
}

bool TaskStateHandler::fillInstanceInfo(const proto::PartitionId _pid, const config::TaskTarget& target,
                                        Task::InstanceInfo& instanceInfo)
{
    instanceInfo.partitionCount = target.getPartitionCount();
    instanceInfo.instanceCount = target.getParallelNum();
    RangeUtil::getParallelInfoByRange(0, 65535, instanceInfo.partitionCount, instanceInfo.instanceCount, _pid.range(),
                                      instanceInfo.partitionId, instanceInfo.instanceId);
    return true;
}

bool TaskStateHandler::needSuspendTask(const std::string& state)
{
    proto::TaskTarget target;
    if (!target.ParseFromString(state)) {
        string errorMsg = "failed to deserialize task target state[" + state + "]";
        FILL_ERRORINFO(SERVICE_TASK_ERROR, errorMsg, BS_STOP);
        return false;
    }
    if (target.suspendtask()) {
        BS_LOG(WARN, "suspend task cmd received!");
        return true;
    }
    return false;
}

bool TaskStateHandler::needRestartProcess(const string& state)
{
    proto::TaskTarget target;
    if (!target.ParseFromString(state)) {
        string errorMsg = "failed to deserialize task target state[" + state + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (checkUpdateConfig(target)) {
        return true;
    }

    ScopedLock lock(_lock);
    if (!_currentTarget.has_taskidentifier()) {
        return false;
    }
    std::string targetTaskIdentifier;
    if (target.has_taskidentifier()) {
        targetTaskIdentifier = target.taskidentifier();
    }
    if (_currentTarget.taskidentifier() != targetTaskIdentifier) {
        BS_LOG(ERROR, "taskIdentifier is not match, current [%s], target [%s], should restart process.",
               _currentTarget.taskidentifier().c_str(), targetTaskIdentifier.c_str());
        return true;
    }
    return false;
}

bool TaskStateHandler::checkUpdateConfig(const proto::TaskTarget& target)
{
    if (!target.has_configpath() || _configPath.empty()) {
        return false;
    }
    string targetConfigPath = target.configpath();
    if (targetConfigPath != _configPath) {
        BS_LOG(INFO, "worker need restart for update config, old configPath[%s], new configPath[%s]",
               _configPath.c_str(), targetConfigPath.c_str());
        return true;
    }
    return false;
}

TaskPtr TaskStateHandler::createTask(const config::TaskTarget& target)
{
    string taskId = _pid.taskid();
    proto::TaskIdentifier taskIdentifier;
    if (!taskIdentifier.fromString(taskId)) {
        string errorMsg = "parse taskId [" + taskId + "] failed";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg, BS_FATAL_ERROR);
        return TaskPtr();
    }
    string taskName;
    if (!taskIdentifier.getTaskName(taskName)) {
        string errorMsg = "get task name failed:taskId [" + taskId + "]";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg, BS_FATAL_ERROR);
        return TaskPtr();
    }

    config::TaskConfig taskConfig;
    string taskConfigPath;
    if (target.getTaskConfigPath(taskConfigPath) && !taskConfigPath.empty()) {
        if (!TaskConfig::loadFromFile(taskConfigPath, taskConfig)) {
            string errorMsg = "get task config from file:[" + taskConfigPath + "]failed";
            FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return TaskPtr();
        }
    }

    string pluginPath = _resourceReader->getPluginPath();
    if (TaskConfig::isBuildInTask(taskName)) {
        pluginPath.clear();
        taskConfig.prepareBuildServiceTaskModuleInfo(_resourceReader, pluginPath);
    }

    if (_reusePluginManager == false || _plugInManager == nullptr) {
        _plugInManager.reset(new plugin::PlugInManager(_resourceReader, pluginPath, plugin::MODULE_FUNC_TASK));
        if (!_plugInManager->addModules(taskConfig.getModuleInfos())) {
            string errorMsg = "add module for  [" + taskName + "] fail";
            FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return TaskPtr();
            return TaskPtr();
        }
    }

    auto taskFactory = getTaskFactory(taskConfig.getTaskModuleName());
    if (!taskFactory) {
        string errorMsg = "create task [" + taskName + "] failed";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg, BS_FATAL_ERROR);
        return TaskPtr();
    }
    TaskPtr task = taskFactory->createTask(taskName);
    if (!task) {
        string errorMsg = "create task [" + taskName + "] failed";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg, BS_FATAL_ERROR);
        return TaskPtr();
    }

    Task::TaskInitParam param;
    if (!fillInstanceInfo(_pid, target, param.instanceInfo)) {
        return TaskPtr();
    }
    param.buildId.appName = _pid.buildid().appname();
    param.buildId.generationId = _pid.buildid().generationid();
    param.buildId.dataTable = _pid.buildid().datatable();
    param.clusterName = _pid.clusternames(0);
    param.resourceReader = _resourceReader;
    param.metricProvider = _metricProvider;
    param.pid = _pid;
    param.appZkRoot = _appZkRoot;
    param.epochId = _epochId;
    param.address = _current.longaddress();
    task->initBeeperTags(param);
    if (!prepareInputs(taskConfig.getInputConfigs(), param.inputCreators)) {
        return TaskPtr();
    }

    if (!prepareOutputs(taskConfig.getOutputConfigs(), param.outputCreators)) {
        return TaskPtr();
    }

    if (!task->init(param)) {
        string errorMsg = "create task [" + taskName + "] failed";
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg, BS_RETRY);
        fillErrorInfo(task.get());
        return TaskPtr();
    }

    string msg = "create task [" + taskName + "] success.";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    return task;
}

io::InputCreatorPtr TaskStateHandler::createInputCreator(const TaskInputConfig& inputConfig)
{
    const auto& moduleName = inputConfig.getModuleName();
    auto module = _plugInManager->getModule(moduleName);
    if (!module) {
        string errorMsg = "Init Task Factory failed. no module name[" + moduleName + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return io::InputCreatorPtr();
    }
    auto factory = dynamic_cast<TaskFactory*>(module->getModuleFactory());
    if (!factory) {
        string errorMsg = "Invalid module[" + moduleName + "].";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return io::InputCreatorPtr();
    }
    return factory->getInputCreator(inputConfig);
}

io::OutputCreatorPtr TaskStateHandler::createOutputCreator(const TaskOutputConfig& outputConfig)
{
    const auto& moduleName = outputConfig.getModuleName();
    BS_LOG(INFO, "create output with module name:[%s]", moduleName.c_str());
    auto module = _plugInManager->getModule(moduleName);
    if (!module) {
        string errorMsg = "Init Task Factory failed. no module name[" + moduleName + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return io::OutputCreatorPtr();
    }
    auto factory = dynamic_cast<TaskFactory*>(module->getModuleFactory());
    if (!factory) {
        string errorMsg = "Invalid module[" + moduleName + "].";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return io::OutputCreatorPtr();
    }
    return factory->getOutputCreator(outputConfig);
}

bool TaskStateHandler::prepareInputs(const map<std::string, TaskInputConfig>& inputConfigs,
                                     task_base::InputCreatorMap& inputMap)
{
    for (const auto& kv : inputConfigs) {
        auto inputCreator = createInputCreator(kv.second);
        if (!inputCreator) {
            string errorMsg = "create task input failed";
            FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg, BS_RETRY);
            return false;
        }
        inputMap[kv.first] = inputCreator;
    }
    return true;
}

bool TaskStateHandler::prepareOutputs(const map<std::string, TaskOutputConfig>& outputConfigs,
                                      task_base::OutputCreatorMap& outputMap)
{
    for (const auto& kv : outputConfigs) {
        auto outputCreator = createOutputCreator(kv.second);
        if (!outputCreator) {
            string errorMsg = "create task output failed";
            FILL_ERRORINFO(SERVICE_ERROR_CONFIG, errorMsg, BS_RETRY);
            return false;
        }
        outputMap[kv.first] = outputCreator;
    }
    return true;
}

void TaskStateHandler::getCurrentState(std::string& state)
{
    ScopedLock lock(_lock);
    if (isSuspended()) {
        _current.set_issuspended(true);
    }
    fillCurrentState(_current);
    fillProgressStatus(_current.status(), _current);
    fillProtocolVersion(_current);
    fillCpuSpeed(_current);
    fillNetworkTraffic(_current);
    if (_task) {
        _current.set_statusdescription(_task->getTaskStatus());
        syncCounter();
        if (_task->hasFatalError()) {
            _task->handleFatalError();
        }
    }
    saveCurrent(_current, state);
    _current.clear_errorinfos();
}

void TaskStateHandler::fillCurrentState(proto::TaskCurrent& current)
{
    fillErrorInfo(_task.get());
    for (size_t i = 0; i < _errorInfos.size(); i++) {
        *current.add_errorinfos() = _errorInfos[i];
    }
    fillWorkerStatus(_task.get(), current);
}

void TaskStateHandler::fillWorkerStatus(Task* task, TaskCurrent& current)
{
    if (isSuspended()) {
        _current.set_status(proto::WS_SUSPENDED);
        BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, "task worker suspended");
        return;
    }
    if (!task) {
        return;
    }
    string targetDescription;
    if (_currentTarget.has_targetdescription()) {
        targetDescription = _currentTarget.targetdescription();
    }

    config::TaskTarget taskTarget;
    try {
        FromJsonString(taskTarget, targetDescription);
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "jsonize targetDescription failed, original str : [%s]", targetDescription.c_str());
        return;
    }
    taskTarget.setTargetTimestamp(_currentTarget.targettimestamp());
    if (task->isDone(taskTarget)) {
        BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, "task worker stopped");
        current.set_status(proto::WS_STOPPED);
        *current.mutable_reachedtarget() = _currentTarget;
    } else {
        current.set_status(proto::WS_STARTED);
    }
}

void TaskStateHandler::fillProgressStatus(proto::WorkerStatus status, TaskCurrent& current)
{
    auto progressStatus = current.mutable_progressstatus();
    progressStatus->set_reporttimestamp(TimeUtility::currentTime());
    if (status == proto::WS_UNKNOWN) {
        return;
    }
    progressStatus->set_starttimestamp(_startTimestamp);

    if (_task) {
        auto taskProgress = _task->getTaskProgress();

        if (taskProgress.has_progress()) {
            progressStatus->set_progress(taskProgress.progress());
        }
        if (taskProgress.has_startpoint()) {
            progressStatus->set_startpoint(taskProgress.startpoint());
        }
        if (taskProgress.has_locatortimestamp()) {
            progressStatus->set_locatortimestamp(taskProgress.locatortimestamp());
        }
    }
}

void TaskStateHandler::syncCounter()
{
    if (_counterSynchronizer) {
        return;
    }

    if (!_task || !_counterConfig) {
        return;
    }

    CounterMapPtr counterMap = _task->getCounterMap();
    if (!counterMap) {
        return;
    }

    if (counterMap) {
        _counterSynchronizer.reset(common::CounterSynchronizerCreator::create(counterMap, _counterConfig));
        if (!_counterSynchronizer) {
            BS_LOG(ERROR, "fail to init counterSynchronizer");
        } else {
            if (!_counterSynchronizer->beginSync()) {
                BS_LOG(ERROR, "fail to start sync counter thread");
                _counterSynchronizer.reset();
            }
        }
    }
}

bool TaskStateHandler::getCounterConfig(const config::ResourceReaderPtr& resourceReader,
                                        config::CounterConfigPtr& counterConfig)
{
    BuildServiceConfig serviceConfig;
    if (!resourceReader->getConfigWithJsonPath("build_app.json", "", serviceConfig)) {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "failed to parse build_app.json", BS_RETRY);
        return false;
    }
    counterConfig.reset(new CounterConfig());
    *counterConfig = serviceConfig.counterConfig;
    if (!overWriteCounterConfig(_pid, *counterConfig)) {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "rewrite CounterConfig failed", BS_STOP);
        return false;
    }
    return true;
}

bool TaskStateHandler::checkTargetTimestamp(const proto::TaskTarget& target)
{
    ScopedLock lock(_lock);
    if (!target.has_targettimestamp()) {
        return false;
    }

    if (!_currentTarget.has_targettimestamp()) {
        return true;
    }

    return target.targettimestamp() >= _currentTarget.targettimestamp();
}

void TaskStateHandler::doHandleTargetState(const string& state, bool hasResourceUpdated)
{
    if (!_pid.has_taskid()) {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "pid not has taskid, create task failed", BS_FATAL_ERROR);
        return;
    }

    proto::TaskTarget target;
    if (!target.ParseFromString(state)) {
        string errorMsg = "failed to deserialize builder target state[" + state + "]";
        FILL_ERRORINFO(SERVICE_TASK_ERROR, errorMsg, BS_STOP);
        return;
    }
    BS_INTERVAL_LOG(60, INFO, "target status: %s", target.ShortDebugString().c_str());
    if (!target.has_targetdescription()) {
        BS_LOG(ERROR, "target has no target description:%s", state.c_str());
        return;
    }

    if (!checkTargetTimestamp(target)) {
        string errorMsg = "target [" + target.ShortDebugString() + "]is older than current target[" +
                          _currentTarget.ShortDebugString() + "]";
        FILL_ERRORINFO(SERVICE_TASK_ERROR, errorMsg, BS_RETRY);
        return;
    }

    if (target == _currentTarget) {
        // do nothing
        return;
    }

    if (!target.has_configpath()) {
        BS_LOG(INFO, "target has no config path, do nothing");
        return;
    }

    // update config
    if (_configPath != target.configpath()) {
        string remoteConfigPath = target.configpath();
        string localConfigPath = downloadConfig(remoteConfigPath);
        if (localConfigPath.empty()) {
            FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "download config fail", BS_RETRY);
            return;
        }
        ScopedLock lock(_lock);
        _configPath = remoteConfigPath;
        _counterSynchronizer.reset();
        _task.reset();
        _resourceReader = ResourceReaderManager::getResourceReader(localConfigPath);
    }

    config::TaskTarget taskTarget;
    try {
        FromJsonString(taskTarget, target.targetdescription());
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "jsonize targetDescription failed, original str : [%s]", target.targetdescription().c_str());
        return;
    }

    TaskPtr task = _task;
    if (!task) {
        if (!getCounterConfig(_resourceReader, _counterConfig)) {
            return;
        }
        task = createTask(taskTarget);
        if (!task) {
            return;
        }
    }

    {
        ScopedLock lock(_lock);
        _task = task;
        _currentTarget = target;
        if (!target.has_taskidentifier()) {
            BS_LOG(WARN, "no task identfier in TaskTarget, should update bs admin.");
        }
        taskTarget.setTargetTimestamp(_currentTarget.targettimestamp());
        if (_startTimestamp == -1) {
            _startTimestamp = TimeUtility::currentTime();
        }
    }
    if (!_task->handleTarget(taskTarget)) {
        ScopedLock lock(_lock);
        _currentTarget = proto::TaskTarget();
        _task->handleFatalError();
    }
}

}} // namespace build_service::worker
