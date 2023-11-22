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
#include "build_service/admin/taskcontroller/BuildServiceTask.h"

#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <map>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "beeper/beeper.h"
#include "build_service/admin/TaskStatusMetricReporter.h"
#include "build_service/admin/taskcontroller/ProcessorTask.h"
#include "build_service/admin/taskcontroller/ProcessorTaskWrapper.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/proto/ProtoUtil.h"
#include "indexlib/misc/common.h"
#include "kmonitor/client/core/MetricsTags.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace beeper;
using namespace build_service::proto;
using namespace autil::legacy::json;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, BuildServiceTask);

const std::string BuildServiceTask::PROCESSOR = "processor";
const std::string BuildServiceTask::BUILDER = "builder";
const std::string BuildServiceTask::MERGER = "merger";
const std::string BuildServiceTask::JOB_BUILDER = "job_builder";
const std::string BuildServiceTask::ALTER_FIELD = "alter_field";

BuildServiceTask::~BuildServiceTask() {}

bool BuildServiceTask::initResource(const KeyValueMap& kvMap)
{
    string dependResourceStr;
    if (!getValueFromKVMap(kvMap, "depends", dependResourceStr)) {
        return true;
    }

    vector<string> dependResources;
    try {
        FromJsonString(dependResources, dependResourceStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "depends[%s] is invalid.", dependResourceStr.c_str());
        return false;
    }
    return fillResourceKeepers(dependResources);
}

bool BuildServiceTask::fillResourceKeepers(const std::vector<std::string>& resourceNames)
{
    for (auto& resource : resourceNames) {
        auto keeper = _resourceManager->applyResource(_taskId, resource, _taskImpl->getConfigReader());
        if (!keeper) {
            BS_LOG(ERROR, "no depend resource [%s].", resource.c_str());
            return false;
        }
        _resourceKeepers.push_back(keeper);
    }
    return true;
}

bool BuildServiceTask::doInit(const KeyValueMap& kvMap)
{
    string buildIdStr;
    if (!getValueFromKVMap(kvMap, "buildId", buildIdStr)) {
        BS_LOG(ERROR, "create task id [%s], type [%s] task param no buildId info", _taskId.c_str(), _taskType.c_str());
        return false;
    }
    proto::BuildId buildId;
    if (!ProtoUtil::strToBuildId(buildIdStr, buildId)) {
        BS_LOG(ERROR, "deserialize failed, create task [%s] fail", _taskType.c_str());
        return false;
    }
    createGlobalTag(buildId, _taskId, kvMap, _beeperTags);

    if (_taskType == PROCESSOR) {
        _taskImpl = createProcessor(buildId, kvMap);
    } else if (_taskType == BUILDER) {
        _taskImpl = createBuilder(buildId, kvMap);
    } else if (_taskType == MERGER) {
        _taskImpl = createMerger(buildId, kvMap);
    } else if (_taskType == JOB_BUILDER) {
        _taskImpl = createJobBuilder(buildId, kvMap);
    } else if (_taskType == ALTER_FIELD) {
        _taskImpl = createAlterField(buildId, kvMap);
    } else {
        _taskImpl = createTask(buildId, kvMap);
    }

    if (!_taskImpl) {
        return false;
    }
    if (!initResource(kvMap)) {
        return false;
    }
    return true;
}

void BuildServiceTask::createGlobalTag(const proto::BuildId& buildId, const string& taskId, const KeyValueMap& kvMap,
                                       EventTagsPtr& beeperTags)
{
    ProtoUtil::buildIdToBeeperTags(buildId, *beeperTags);
    beeperTags->AddTag("taskId", taskId);
    string buildStep;
    if (getValueFromKVMap(kvMap, "buildStep", buildStep)) {
        beeperTags->AddTag("buildStep", buildStep);
        _kmonTags.AddTag("buildStep", buildStep);
    }

    string clusterName;
    if (getValueFromKVMap(kvMap, "clusterName", clusterName)) {
        beeperTags->AddTag("clusters", clusterName);
        _kmonTags.AddTag("clusters", clusterName);
    }
}

bool BuildServiceTask::getInitParam(const KeyValueMap& kvMap, proto::BuildStep& buildStep, string& clusterName)
{
    if (!getValueFromKVMap(kvMap, "clusterName", clusterName)) {
        BS_LOG(ERROR,
               "create Builder for task id [%s], type [%s] fail,"
               "no clusterName parameter",
               _taskId.c_str(), _taskType.c_str());
        return false;
    }
    if (!getBuildStep(kvMap, buildStep)) {
        return false;
    }
    return true;
}

BuilderTaskWrapperPtr BuildServiceTask::createBuilder(const proto::BuildId& buildId, const KeyValueMap& kvMap)
{
    if (!getInitParam(kvMap, _buildStep, _clusterName)) {
        return BuilderTaskWrapperPtr();
    }
    BuilderTaskWrapperPtr builder(new BuilderTaskWrapper(buildId, _clusterName, _taskId, _resourceManager));
    builder->setBeeperTags(_beeperTags);
    if (!builder->init(_buildStep)) {
        BS_LOG(ERROR, "init builder failed");
        return BuilderTaskWrapperPtr();
    }
    setProperty("batchMode", builder->isBatchMode() ? "true" : "false");
    return builder;
}

SingleJobBuilderTaskPtr BuildServiceTask::createJobBuilder(const proto::BuildId& buildId, const KeyValueMap& kvMap)
{
    if (!getValueFromKVMap(kvMap, "clusterName", _clusterName)) {
        BS_LOG(ERROR,
               "create JobBuilder for task id [%s], type [%s] fail,"
               "no clusterName parameter",
               _taskId.c_str(), _taskType.c_str());
        return SingleJobBuilderTaskPtr();
    }

    string dataSource;
    if (!getValueFromKVMap(kvMap, "dataDescriptions", dataSource)) {
        BS_LOG(ERROR, "job builder no datasource");
        return SingleJobBuilderTaskPtr();
    }
    DataDescriptions descriptions;
    FromJsonString(descriptions.toVector(), dataSource);

    SingleJobBuilderTaskPtr job(new SingleJobBuilderTask(buildId, _clusterName, _resourceManager));
    if (!job->init(descriptions)) {
        BS_LOG(ERROR, "init JobBuilder failed");
        return SingleJobBuilderTaskPtr();
    }
    return job;
}

SingleMergerTaskPtr BuildServiceTask::createMerger(const proto::BuildId& buildId, const KeyValueMap& kvMap)
{
    if (!getInitParam(kvMap, _buildStep, _clusterName)) {
        return SingleMergerTaskPtr();
    }
    string mergeConfigName;
    auto iter = kvMap.find("mergeConfigName");
    if (iter != kvMap.end()) {
        mergeConfigName = iter->second;
    }

    string batchMaskStr;
    int32_t batchMask = -1;
    if (getValueFromKVMap(kvMap, "batchMask", batchMaskStr)) {
        if (!StringUtil::fromString(batchMaskStr, batchMask)) {
            batchMask = -1;
        }
        BS_LOG(INFO, "create merger with batch mask[%d] for [%s]", batchMask, ProtoUtil::buildIdToStr(buildId).c_str());
    }
    SingleMergerTaskPtr merger(new SingleMergerTask(buildId, _clusterName, _resourceManager));
    merger->setBeeperTags(_beeperTags);
    if (!merger->init(_buildStep, mergeConfigName, batchMask)) {
        BS_LOG(ERROR, "init merger failed");
        return SingleMergerTaskPtr();
    }
    return merger;
}

AlterFieldTaskPtr BuildServiceTask::createAlterField(const proto::BuildId& buildId, const KeyValueMap& kvMap)
{
    if (!getInitParam(kvMap, _buildStep, _clusterName)) {
        return AlterFieldTaskPtr();
    }
    AlterFieldTaskPtr alterFieldTask(new AlterFieldTask(buildId, _clusterName, _resourceManager));
    alterFieldTask->setBeeperTags(_beeperTags);
    if (!alterFieldTask->init(_buildStep)) {
        BS_LOG(ERROR, "init alterFieldTask failed");
        return AlterFieldTaskPtr();
    }
    return alterFieldTask;
}

bool BuildServiceTask::getBuildStep(const KeyValueMap& kvMap, proto::BuildStep& buildStep)
{
    string buildStepStr;
    if (!getValueFromKVMap(kvMap, "buildStep", buildStepStr)) {
        BS_LOG(ERROR, "parmeter no buildStep");
        return false;
    }
    if (buildStepStr == "full") {
        buildStep = BUILD_STEP_FULL;
    } else {
        buildStep = BUILD_STEP_INC;
    }
    return true;
}

void BuildServiceTask::setTaskStatus(TaskStatus stat)
{
    TaskBase::setTaskStatus(stat);
    if (stat == TaskBase::ts_stopped) {
        _taskImpl->notifyStopped();
    }

    if (stat == TaskBase::ts_stopped || stat == TaskBase::ts_finish) {
        _nodeInfo.totalCnt = 0;
        _nodeInfo.finishCnt = 0;
    }
}

TaskMaintainerPtr BuildServiceTask::createTask(const proto::BuildId& buildId, const KeyValueMap& kvMap)
{
    TaskMaintainerPtr taskMaintainer(new TaskMaintainer(buildId, _taskId, _taskType, _resourceManager));
    if (!getValueFromKVMap(kvMap, "clusterName", _clusterName)) {
        BS_LOG(ERROR,
               "create Task for task id [%s], type [%s] fail,"
               "no clusterName parameter",
               _taskId.c_str(), _taskType.c_str());
        return TaskMaintainerPtr();
    }

    string taskConfigPath;
    getValueFromKVMap(kvMap, "taskConfigPath", taskConfigPath);

    string taskParam;
    getValueFromKVMap(kvMap, "taskParam", taskParam);
    _beeperTags->AddTag("taskName", _taskType);
    taskMaintainer->setBeeperTags(_beeperTags);
    if (!taskMaintainer->init(_clusterName, taskConfigPath, taskParam)) {
        return TaskMaintainerPtr();
    }
    return taskMaintainer;
}

AdminTaskBasePtr BuildServiceTask::createProcessor(const proto::BuildId& buildId, const KeyValueMap& kvMap)
{
    if (!getInitParam(kvMap, _buildStep, _clusterName)) {
        return AdminTaskBasePtr();
    }

    ProcessorTaskPtr processor(new ProcessorTask(buildId, _buildStep, _resourceManager));
    if (!processor->init(kvMap)) {
        BS_LOG(ERROR, "init processor task failed");
        return AdminTaskBasePtr();
    }
    _batchMode = processor->isBatchMode();
    if (_batchMode) {
        // batch mode, dont optimize processor task
        processor->setBeeperTags(_beeperTags);
        return processor;
    }

    bool disableOptimize = false;
    string optimizeStr;
    if (getValueFromKVMap(kvMap, "disableOptimize", optimizeStr)) {
        disableOptimize = (optimizeStr == "true");
    }
    ProcessorTaskWrapperPtr taskWrapper(new ProcessorTaskWrapper(_clusterName, processor, disableOptimize));
    taskWrapper->setBeeperTags(_beeperTags);
    return taskWrapper;
}

bool BuildServiceTask::doFinish(const KeyValueMap& kvMap)
{
    // todo: add
    return _taskImpl->finish(kvMap);
}

void BuildServiceTask::doSyncTaskProperty(WorkerNodes& workerNodes)
{
    _taskImpl->run(workerNodes);
    string curPhaseId = _taskImpl->getTaskPhaseIdentifier();
    if (curPhaseId != _taskPhaseId) {
        _taskPhaseId = curPhaseId;
        _kmonTags.DelTag("task_phase");
        _kmonTags.AddTag("task_phase", curPhaseId);
    }

    const auto& propertyKvMap = _taskImpl->GetPropertyMap();
    for (const auto& kv : propertyKvMap) {
        setProperty(kv.first, kv.second);
    }

    size_t totalCnt = workerNodes.size();
    size_t finishCnt = 0;
    for (auto node : workerNodes) {
        if (node->isFinished()) {
            finishCnt++;
        }
    }

    for (auto resourceKeeper : _resourceKeepers) {
        resourceKeeper->fillResourceKeeper(workerNodes);
    }

    _nodeInfo.totalCnt = totalCnt;
    _nodeInfo.finishCnt = finishCnt;
    int64_t runningCnt = totalCnt;
    if (isTaskFinishing() || (finishCnt > 0 && finishCnt != totalCnt)) {
        runningCnt = totalCnt - finishCnt;
        int64_t now = autil::TimeUtility::currentTimeInSeconds();
        if (now - _beeperReportTs > _beeperReportInterval) {
            BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                                 "%s:totalNodeCount[%ld], unfinishNodeCount[%ld]", _taskType.c_str(), totalCnt,
                                 runningCnt);
            for (auto node : workerNodes) {
                if (!node->isFinished()) {
                    string roleId;
                    ProtoUtil::partitionIdToRoleId(node->getPartitionId(), roleId);
                    BS_LOG(INFO, "role [%s] is unfinished, latest heartbeat timestamp [%ld], spec identifier [%s]",
                           roleId.c_str(), node->getHeartbeatUpdateTime(), node->getCurrentIdentifier().c_str());
                }
            }
            _beeperReportTs = now;
        }
    }

    TaskStatusMetricReporterPtr reporter;
    if (_resourceManager) {
        _resourceManager->getResource(reporter);
    }
    if (reporter) {
        reporter->reportTaskNodeCount(totalCnt, runningCnt, _kmonTags);
        int64_t interval;
        if (_taskImpl->getTaskRunningTime(interval)) {
            reporter->reportTaskRunningTime(interval, _kmonTags);
        }
    }
}

bool BuildServiceTask::isFinished(WorkerNodes& workerNodes)
{
    return _taskImpl->getTaskStatus() == AdminTaskBase::TASK_FINISHED;
}

TaskBase* BuildServiceTask::clone()
{
    TaskBase* taskCopy = new BuildServiceTask(*this);
    return taskCopy;
}

void BuildServiceTask::doAccept(const TaskOptimizerPtr& optimizer, TaskOptimizer::OptimizeStep step)
{
    if (step == TaskOptimizer::COLLECT) {
        optimizer->collect(_taskImpl);
        return;
    }
    _taskImpl = optimizer->optimize(_taskImpl);
}

void BuildServiceTask::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("cluster_name", _clusterName, _clusterName);
    json.Jsonize("build_step", _buildStep);
    json.Jsonize("batch_mode", _batchMode, _batchMode);
    if (json.GetMode() == TO_JSON) {
        proto::BuildId buildId = _taskImpl->getBuildId();
        string buildIdStr = ProtoUtil::buildIdToStr(buildId);
        json.Jsonize("build_id", buildIdStr);
        _taskImpl->Jsonize(json);
        vector<string> dependResources;
        dependResources.reserve(_resourceKeepers.size());
        for (size_t i = 0; i < _resourceKeepers.size(); i++) {
            dependResources.push_back(_resourceKeepers[i]->getResourceName());
        }
        json.Jsonize("depend_resources", dependResources, dependResources);
    } else {
        string buildIdStr;
        json.Jsonize("build_id", buildIdStr);
        proto::BuildId buildId;
        if (!ProtoUtil::strToBuildId(buildIdStr, buildId)) {
            BS_LOG(ERROR, "deserialize buildId from [%s] failed", buildIdStr.c_str());
            throw autil::legacy::ExceptionBase("deserialize buildId fail");
        }
        _beeperTags.reset(new EventTags);
        ProtoUtil::buildIdToBeeperTags(buildId, *_beeperTags);
        _beeperTags->AddTag("taskId", _taskId);
        _beeperTags->AddTag("buildStep", ProtoUtil::toStepString(_buildStep));
        _beeperTags->AddTag("clusters", _clusterName);
        _kmonTags.AddTag("buildStep", ProtoUtil::toStepString(_buildStep));
        _kmonTags.AddTag("clusters", _clusterName);
        if (_taskType == PROCESSOR) {
            if (_batchMode) {
                _taskImpl.reset(new ProcessorTask(buildId, _buildStep, _resourceManager));
            } else {
                ProcessorTaskPtr processorTask(new ProcessorTask(buildId, _buildStep, _resourceManager));
                _taskImpl.reset(new ProcessorTaskWrapper(_clusterName, processorTask));
            }
        } else if (_taskType == BUILDER) {
            _taskImpl.reset(new BuilderTaskWrapper(buildId, _clusterName, _taskId, _resourceManager));
        } else if (_taskType == MERGER) {
            _taskImpl.reset(new SingleMergerTask(buildId, _clusterName, _resourceManager));
        } else if (_taskType == JOB_BUILDER) {
            _taskImpl.reset(new SingleJobBuilderTask(buildId, _clusterName, _resourceManager));
        } else if (_taskType == ALTER_FIELD) {
            _taskImpl.reset(new AlterFieldTask(buildId, _clusterName, _resourceManager));
        } else {
            _taskImpl.reset(new TaskMaintainer(buildId, _taskId, _taskType, _resourceManager));
            _beeperTags->AddTag("taskName", _taskType);
        }
        _taskImpl->Jsonize(json);
        _taskImpl->setBeeperTags(_beeperTags);
        _taskImpl->SetPropertyMap(_propertyMap);
        _kmonTags.AddTag("task_phase", _taskImpl->getTaskPhaseIdentifier());
        vector<string> dependResources;
        json.Jsonize("depend_resources", dependResources, dependResources);
        if (!fillResourceKeepers(dependResources)) {
            throw autil::legacy::ExceptionBase("recover resource keeper failed.");
        }
    }
}

std::string BuildServiceTask::getNodesIdentifier() const
{
    if (_taskType == PROCESSOR) {
        assert(_taskImpl);
        if (_batchMode) {
            return ((ProcessorTask*)(_taskImpl.get()))->getTaskIdentifier();
        } else {
            return ((ProcessorTaskWrapper*)(_taskImpl.get()))->getIdentifier();
        }
    }
    return TaskBase::getIdentifier();
}

bool BuildServiceTask::doSuspend()
{
    _taskImpl->suspendTask(false);
    return true;
}

bool BuildServiceTask::doResume()
{
    _taskImpl->resumeTask();
    return true;
}

bool BuildServiceTask::waitSuspended(WorkerNodes& workerNodes)
{
    if (_taskImpl->getTaskStatus() != AdminTaskBase::TaskStatus::TASK_SUSPENDING) {
        // when recover from zk, a suspending task status will be normal
        // recover nodes first
        doSuspend();
    }
    if (workerNodes.size() == 0) {
        doSyncTaskProperty(workerNodes);
    }
    _taskImpl->waitSuspended(workerNodes);
    return _taskImpl->isTaskSuspended();
}

bool BuildServiceTask::updateConfig() { return _taskImpl->updateConfig(); }

void BuildServiceTask::clearWorkerZkNode(const string& generationDir) const
{
    // processor builder merger only clear full zk node
    if (_taskType == PROCESSOR) {
        if (_batchMode) {
            ProcessorTaskPtr task = DYNAMIC_POINTER_CAST(ProcessorTask, _taskImpl);
            task->clearFullWorkerZkNode(generationDir);
            task->clearCounterAndIncZkNode(generationDir);
        } else {
            ProcessorTaskWrapperPtr task = DYNAMIC_POINTER_CAST(ProcessorTaskWrapper, _taskImpl);
            task->clearFullWorkerZkNode(generationDir);
        }
    } else if (_taskType == BUILDER) {
        BuilderTaskWrapperPtr task = DYNAMIC_POINTER_CAST(BuilderTaskWrapper, _taskImpl);
        task->clearFullWorkerZkNode(generationDir);
    } else if (_taskType == MERGER) {
        SingleMergerTaskPtr task = DYNAMIC_POINTER_CAST(SingleMergerTask, _taskImpl);
        task->clearFullWorkerZkNode(generationDir);
    } else if (_taskType == JOB_BUILDER) {
        SingleJobBuilderTaskPtr task = DYNAMIC_POINTER_CAST(SingleJobBuilderTask, _taskImpl);
        task->clearFullWorkerZkNode(generationDir);
    } else if (_taskType == ALTER_FIELD) {
        // do nothing
    } else {
        TaskMaintainerPtr task = DYNAMIC_POINTER_CAST(TaskMaintainer, _taskImpl);
        task->cleanZkTaskNodes(generationDir);
    }
}

void BuildServiceTask::supplementLableInfo(KeyValueMap& info) const
{
    info["clusterName"] = _clusterName;
    if (_buildStep != NO_BUILD_STEP && _buildStep != BUILD_STEP_IDLE) {
        info["buildStep"] =
            (_buildStep == proto::BUILD_STEP_FULL) ? config::BUILD_STEP_FULL_STR : config::BUILD_STEP_INC_STR;
    }
    info["workNodeCount"] = string("total:") + StringUtil::toString(_nodeInfo.totalCnt) +
                            ";finish:" + StringUtil::toString(_nodeInfo.finishCnt);
    if (_taskImpl) {
        _taskImpl->supplementLableInfo(info);
    }
}

}} // namespace build_service::admin
