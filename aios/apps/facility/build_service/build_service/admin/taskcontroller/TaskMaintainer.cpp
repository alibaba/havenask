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
#include "build_service/admin/taskcontroller/TaskMaintainer.h"

#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <stdint.h>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "beeper/beeper.h"
#include "beeper/common/common_type.h"
#include "build_service/admin/SlowNodeDetector.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/admin/taskcontroller/BuildinTaskControllerFactory.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/WorkerNodeCreator.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/misc/common.h"

using namespace std;
using namespace autil;
using namespace build_service::proto;
using namespace beeper;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, TaskMaintainer);

TaskMaintainer::TaskMaintainer(const BuildId& buildId, const string& taskId, const string& taskName,
                               const TaskResourceManagerPtr& resMgr)
    : AdminTaskBase(buildId, resMgr)
    , _taskId(taskId)
    , _taskName(taskName)
    , _legacyTaskIdentifyStr(false)
{
}

TaskMaintainer::~TaskMaintainer() {}

void TaskMaintainer::setBeeperTags(const beeper::EventTagsPtr beeperTags)
{
    assert(beeperTags);
    AdminTaskBase::setBeeperTags(beeperTags);
    if (_taskController) {
        _taskController->setBeeperTags(beeperTags);
    }
}

bool TaskMaintainer::init(const std::string& clusterName, const std::string& taskConfigPath,
                          const std::string& initParam)
{
    _taskConfigPath = taskConfigPath;
    _clusterName = clusterName;

    BuildinTaskControllerFactory buildinFactory;
    _taskController = buildinFactory.createTaskController(_taskId, _taskName, _resourceManager);
    _taskController->setBeeperTags(_beeperTags);
    if (!_taskController) {
        return false;
    }

    // TODO: refactor to one configReader
    config::ResourceReaderPtr resourceReader = getResourceReader();
    string validTaskConfigPath;
    if (!config::TaskConfig::getValidTaskConfigPath(resourceReader->getConfigPath(), taskConfigPath, clusterName,
                                                    _taskName, validTaskConfigPath)) {
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start task [%s] failed: for invalid config", _taskName.c_str());

        return false;
    }

    if (!_taskController->init(clusterName, validTaskConfigPath, initParam)) {
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags,
                             "start task [%s] failed: for task init failed", _taskName.c_str());
        return false;
    }
    _configPath = _taskController->getUsingConfigPath();
    initSlowNodeDetect(resourceReader, _clusterName);
    BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags, "start task [%s] success", _taskName.c_str());
    return true;
}

string TaskMaintainer::getTaskPhaseIdentifier() const { return _taskName; }

void TaskMaintainer::getTaskIdentifier(proto::TaskIdentifier& identifier)
{
    string taskId;
    string flowId;
    TaskFlow::getOriginalFlowIdAndTaskId(_taskId, flowId, taskId);
    identifier.setTaskId(taskId);

    if (_legacyTaskIdentifyStr) {
        identifier.setClusterName(_clusterName);
    }
    identifier.setTaskName(_taskName);
}

string TaskMaintainer::getTaskIdentifier()
{
    TaskIdentifier identifier;
    getTaskIdentifier(identifier);
    return identifier.toString();
}

bool TaskMaintainer::run(WorkerNodes& workerNodes)
{
    TaskNodes taskNodes;
    taskNodes.reserve(workerNodes.size());
    for (const auto& workerNode : workerNodes) {
        taskNodes.push_back(DYNAMIC_POINTER_CAST(TaskNode, workerNode));
    }
    bool ret = run(taskNodes);
    workerNodes.clear();
    workerNodes.reserve(taskNodes.size());
    for (auto taskNode : taskNodes) {
        workerNodes.push_back(taskNode);
    }
    if (workerNodes.size() > 0) {
        _hasCreateNodes = true;
        if (_nodesStartTimestamp == -1) {
            _nodesStartTimestamp = autil::TimeUtility::currentTimeInMicroSeconds();
        }
    }
    if (ret) {
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags, "task [%s] finish", _taskName.c_str());
        _taskStatus = TASK_FINISHED;
        _nodesStartTimestamp = -1;
    }
    return ret;
}

bool TaskMaintainer::existBackInfo(const proto::TaskNodes& taskNodes) const
{
    for (const auto& taskNode : taskNodes) {
        auto pid = taskNode->getPartitionId();
        if (pid.has_backupid()) {
            return true;
        }
    }
    return false;
}

bool TaskMaintainer::run(TaskNodes& taskNodes)
{
    auto needRecoverBackInfo = taskNodes.size() == 0;
    TaskController::Nodes nodes;
    taskNodesToNodes(taskNodes, nodes);
    bool ret = _taskController->operate(nodes);
    auto propertyMap = _taskController->GetPropertyMap();
    for (const auto& kv : propertyMap) {
        SetProperty(kv.first, kv.second);
    }
    nodesToTaskNodes(nodes, taskNodes);

    if (needRecoverBackInfo) {
        recoverFromBackInfo(taskNodes);
    }

    _nodeStatusManager->Update(taskNodes);
    if (!taskNodes.empty()) {
        assert(nodes.size() > 0);
        if (!_fatalErrorDiscoverer) {
            _fatalErrorDiscoverer.reset(new FatalErrorDiscoverer());
        }
        _fatalErrorDiscoverer->collectNodeFatalErrors(taskNodes);
        bool simpleHandle = _taskController->isSupportBackup() ? false : true;
        detectSlowNodes(taskNodes, _clusterName, simpleHandle);

        if (existBackInfo(taskNodes)) {
            saveBackupInfo();
        } else {
            clearBackupInfo();
        }
    }
    return ret;
}

void TaskMaintainer::taskNodesToNodes(const TaskNodes& taskNodes, TaskController::Nodes& nodes)
{
    for (size_t i = 0; i < taskNodes.size(); i++) {
        TaskController::Node node;
        config::TaskTarget target;
        FromJsonString(target, taskNodes[i]->getTargetStatus().targetdescription());
        node.taskTarget = target;
        node.statusDescription = taskNodes[i]->getCurrentStatus().statusdescription();
        ProtoUtil::partitionIdToRoleId(taskNodes[i]->getPartitionId(), node.roleName);
        node.nodeId = taskNodes[i]->getNodeId();
        node.instanceIdx = taskNodes[i]->getInstanceIdx();
        node.sourceNodeId = taskNodes[i]->getSourceNodeId();
        auto pid = taskNodes[i]->getPartitionId();
        if (pid.has_backupid()) {
            node.backupId = pid.backupid();
        } else {
            node.backupId = -1;
        }
        if (taskNodes[i]->getCurrentStatus().reachedtarget() == taskNodes[i]->getTargetStatus()) {
            node.reachedTarget = true;
        } else {
            node.reachedTarget = false;
        }
        if (taskNodes[i]->isSuspended()) {
            node.isSuspended = true;
        } else {
            node.isSuspended = false;
        }
        nodes.push_back(node);
    }
}

void TaskMaintainer::waitSuspended(WorkerNodes& workerNodes)
{
    if (_taskStatus == TASK_SUSPENDED) {
        return;
    }
    bool allSuspended = true;
    TaskNodes activeNodes;
    activeNodes.reserve(workerNodes.size());
    for (auto& workerNode : workerNodes) {
        activeNodes.push_back(DYNAMIC_POINTER_CAST(TaskNode, workerNode));
    }

    for (const auto& node : activeNodes) {
        if (node->isSuspended() || node->isFinished()) {
            continue;
        }
        if (isSuspended(node)) {
            node->setSuspended(true);
        } else {
            allSuspended = false;
            proto::TaskTarget suspendTarget;
            suspendTarget.set_suspendtask(true);
            node->setTargetStatus(suspendTarget);
        }
    }
    if (allSuspended) {
        _taskStatus = TASK_SUSPENDED;
    }
}

std::string TaskMaintainer::getOriginalTaskId() const
{
    std::string flowId;
    std::string id;
    TaskFlow::getOriginalFlowIdAndTaskId(_taskId, flowId, id);
    return id;
}

void TaskMaintainer::collectTaskInfo(proto::TaskInfo* taskInfo, const TaskNodes& taskNodes,
                                     const CounterCollector::RoleCounterMap& roleCounterMap)
{
    string flowId;
    string id;
    TaskFlow::getOriginalFlowIdAndTaskId(_taskId, flowId, id);
    int64_t numTaskId = -1;
    if (StringUtil::fromString(id, numTaskId)) {
        taskInfo->set_taskid(numTaskId);
    }
    taskInfo->set_taskidentifier(id);
    taskInfo->set_taskname(_taskName);
    if (_taskStatus == TASK_FINISHED) {
        taskInfo->set_taskstep("finished");
    } else if (_taskStatus == TASK_STOPPED) {
        taskInfo->set_taskstep("stopped");
    } else {
        taskInfo->set_taskstep("running");
    }

    fillPartitionInfos(taskInfo, taskNodes, roleCounterMap);
    if (_fatalErrorDiscoverer) {
        _fatalErrorDiscoverer->fillFatalErrors(taskInfo->mutable_fatalerrors());
    }
    if (_taskController) {
        taskInfo->set_taskinfo(_taskController->getTaskInfo());
    }
}

bool TaskMaintainer::operator==(const TaskMaintainer& other) const
{
    bool ret =
        _taskName == other._taskName && _clusterName == other._clusterName && _slowNodeDetect == other._slowNodeDetect;
    if (!ret) {
        return false;
    }
    if (_taskController) {
        if (other._taskController) {
            return *_taskController == *(other._taskController);
        } else {
            return false;
        }
    }
    return other._taskController == TaskControllerPtr();
}

bool TaskMaintainer::operator!=(const TaskMaintainer& other) const { return !(*this == other); }

void TaskMaintainer::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("config_path", _configPath, _configPath);
    json.Jsonize("has_create_nodes", _hasCreateNodes, _hasCreateNodes);
    json.Jsonize("task_id", _taskId, _taskId);
    json.Jsonize("task_config_path", _taskConfigPath, _taskConfigPath);
    json.Jsonize("task_name", _taskName, _taskName);
    json.Jsonize("cluster_name", _clusterName, _clusterName);
    json.Jsonize("slow_node_detect", _slowNodeDetect, false);
    json.Jsonize("task_status", _taskStatus, _taskStatus);
    json.Jsonize("use_legacy_task_identifier", _legacyTaskIdentifyStr, true); // TODO: remove later

    if (json.GetMode() == FROM_JSON) {
        BuildinTaskControllerFactory buildinFactory;
        _taskController = buildinFactory.createTaskController(_taskId, _taskName, _resourceManager);
        if (!_taskController) {
            string errorInfo = "create task [" + _taskName + "] controller fail";
            throw autil::legacy::ExceptionBase(errorInfo);
        }
        json.Jsonize("task_controller", *_taskController);
        if (_configPath.empty()) {
            config::ResourceReaderPtr resourceReader = getResourceReader();
            _configPath = resourceReader->getOriginalConfigPath();
        }
        if (!_slowNodeDetect) {
            initSlowNodeDetect(getResourceReader(), _clusterName);
        }
    } else {
        json.Jsonize("task_controller", *_taskController);
    }
}

config::ResourceReaderPtr TaskMaintainer::getResourceReader() const
{
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    return readerAccessor->getLatestConfig();
}

bool TaskMaintainer::updateConfig()
{
    _slowNodeDetector.reset();
    _fatalErrorDiscoverer.reset();
    if (_taskController->updateConfig()) {
        _configPath = _taskController->getUsingConfigPath();
        return true;
    }
    return false;
}

TaskNodePtr TaskMaintainer::createTaskNode(const TaskController::Node& node)
{
    PartitionId pid = createPartitionId(node);
    if (node.backupId != -1) {
        pid.set_backupid(node.backupId);
    }
    TaskNodePtr taskNode(new TaskNode(pid));
    proto::TaskTarget targetStatus;
    targetStatus.set_configpath(_configPath);
    targetStatus.set_targetdescription(ToJsonString(node.taskTarget, /*isCompact=*/true));
    targetStatus.set_targettimestamp(TimeUtility::currentTimeInSeconds());
    targetStatus.set_taskidentifier(_taskId);
    taskNode->setTargetStatus(targetStatus);
    taskNode->setNodeId(node.nodeId);
    taskNode->setInstanceIdx(node.instanceIdx);
    if (node.reachedTarget) {
        taskNode->setFinished(true);
    }
    if (node.isSuspended) {
        taskNode->setSuspended(true);
    } else {
        taskNode->setSuspended(false);
    }
    return taskNode;
}

PartitionId TaskMaintainer::createPartitionId(const TaskController::Node& node)
{
    // todo: when intergrate processor, builder, merger, add correct build step
    TaskIdentifier identifier;
    getTaskIdentifier(identifier);
    if (!node.roleName.empty()) {
        identifier.setValue("name", node.roleName);
    }
    return WorkerNodeCreator<ROLE_TASK>::createPartitionId(
        node.taskTarget.getPartitionCount(), node.taskTarget.getParallelNum(), node.instanceIdx, NO_BUILD_STEP,
        _buildId, _clusterName, "", identifier.toString());
}

void TaskMaintainer::cleanZkTaskNodes(const std::string& generationDir)
{
    std::vector<std::string> fileList;
    fslib::util::FileUtil::listDir(generationDir, fileList);
    for (const auto& fileName : fileList) {
        proto::PartitionId pid;
        proto::ProtoUtil::roleIdToPartitionId(fileName, _buildId.appname(), pid);
        if (!pid.has_role()) {
            continue;
        }

        if (pid.role() != proto::ROLE_TASK) {
            continue;
        }

        if (!pid.has_taskid()) {
            continue;
        }

        TaskIdentifier identifier;
        if (!identifier.fromString(pid.taskid())) {
            continue;
        }
        TaskIdentifier currentIdentifier;
        getTaskIdentifier(currentIdentifier);

        if (identifier.getTaskId() != currentIdentifier.getTaskId()) {
            continue;
        }
        std::string rolePath = fslib::util::FileUtil::joinFilePath(generationDir, fileName);
        if (!fslib::util::FileUtil::remove(rolePath)) {
            BS_LOG(WARN, "remove task node %s failed", rolePath.c_str());
        }
    }
}

void TaskMaintainer::nodesToTaskNodes(const TaskController::Nodes& nodes, TaskNodes& taskNodes)
{
    if (nodes.size() == 0) {
        if (taskNodes.size() > 0) {
            taskNodes.clear();
            _slowNodeDetector.reset();
            _fatalErrorDiscoverer.reset();
        }
        return;
    }

    if (nodes.size() != taskNodes.size()) {
        taskNodes.clear();
        _slowNodeDetector.reset();
        _fatalErrorDiscoverer.reset();
    }

    TaskNodes tmpNodes;
    for (size_t i = 0; i < nodes.size(); i++) {
        auto taskNode = GetTaskNode(taskNodes, nodes[i]);
        if (!taskNode) {
            tmpNodes.push_back(createTaskNode(nodes[i]));
        } else {
            if (nodes[i].reachedTarget) {
                taskNode->setFinished(true);
            } else {
                if (nodes[i].isSuspended) {
                    taskNode->setSuspended(true);
                } else {
                    if (taskNode->isSuspended()) {
                        taskNode->setSuspended(false);
                    }
                }
                proto::TaskTarget target;
                target.set_configpath(_configPath);
                target.set_targetdescription(ToJsonString(nodes[i].taskTarget, /*isCompact=*/true));
                const auto& targetStatus = taskNode->getTargetStatus();
                config::TaskTarget taskTarget;
                FromJsonString(taskTarget, targetStatus.targetdescription());
                if (taskTarget.getTargetName() != nodes[i].taskTarget.getTargetName()) {
                    target.set_targettimestamp(TimeUtility::currentTimeInSeconds());
                } else {
                    target.set_targettimestamp(targetStatus.targettimestamp());
                }
                target.set_taskidentifier(_taskId);
                taskNode->setTargetStatus(target);
            }
            tmpNodes.push_back(taskNode);
        }
    }
    taskNodes.swap(tmpNodes);
}
proto::TaskNodePtr TaskMaintainer::GetTaskNode(const proto::TaskNodes& taskNodes,
                                               const TaskController::Node& node) const
{
    for (auto& taskNode : taskNodes) {
        if (node.sourceNodeId != -1) { // handle backup node
            if (taskNode->getSourceNodeId() == node.sourceNodeId && taskNode->getBackupId() == node.backupId) {
                return taskNode;
            }
        } else {
            if (taskNode->getSourceNodeId() == node.sourceNodeId && taskNode->getNodeId() == node.nodeId) {
                return taskNode;
            }
        }
    }
    return nullptr;
}

bool TaskMaintainer::start(const KeyValueMap& kvMap)
{
    assert(_taskController);
    assert(_beeperTags);
    if (_taskController->start(kvMap)) {
        _taskStatus = TASK_NORMAL;
        _configPath = _taskController->getUsingConfigPath();
        return true;
    }
    return false;
}

void TaskMaintainer::notifyStopped()
{
    assert(_taskController);
    _taskController->notifyStopped();
    AdminTaskBase::notifyStopped();
}

void TaskMaintainer::doSupplementLableInfo(KeyValueMap& info) const
{
    if (_taskController) {
        _taskController->supplementLableInfo(info);
    }

    if (_fatalErrorDiscoverer) {
        _fatalErrorDiscoverer->supplementFatalErrorInfo(info);
    }
}

const TaskControllerPtr& TaskMaintainer::getController() const { return _taskController; }
}} // namespace build_service::admin
