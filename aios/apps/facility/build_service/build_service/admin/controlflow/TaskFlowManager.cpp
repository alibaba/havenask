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
#include "build_service/admin/controlflow/TaskFlowManager.h"

#include "build_service/admin/controlflow/ControlDefine.h"
#include "build_service/admin/controlflow/FlowContainer.h"
#include "build_service/admin/controlflow/FlowFactory.h"
#include "build_service/admin/controlflow/FlowSerializer.h"
#include "build_service/admin/controlflow/GraphBuilder.h"
#include "build_service/admin/controlflow/TaskContainer.h"
#include "build_service/admin/controlflow/TaskSerializer.h"
#include "build_service/admin/taskcontroller/BuildServiceTask.h"
using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace build_service::proto;
namespace build_service { namespace admin {
BS_LOG_SETUP(admin, TaskFlowManager);

#define LOG_PREFIX _logPrefix.c_str()

TaskFlowManager::TaskFlowManager(const string& rootPath, const TaskFactoryPtr& factory,
                                 const TaskResourceManagerPtr& resMgr)
    : _rootPath(rootPath)
{
    _taskContainer.reset(new TaskContainer);
    _flowContainer.reset(new FlowContainer);
    _flowFactory.reset(new FlowFactory(_flowContainer, _taskContainer, factory, resMgr));
}

TaskFlowManager::~TaskFlowManager() {}

bool TaskFlowManager::init(const string& graphFileName, const KeyValueMap& kvMap)
{
    if (graphFileName.empty()) {
        return true;
    }
    return loadSubGraph("", graphFileName, kvMap);
}

void TaskFlowManager::getFlowIdByTag(const std::string& tag, std::vector<std::string>& flowIds)
{
    _flowContainer->getFlowIdByTag(tag, flowIds);
}

void TaskFlowManager::getFlowByTag(const std::string& tag, std::vector<TaskFlowPtr>& flows)
{
    vector<string> flowIds;
    getFlowIdByTag(tag, flowIds);
    for (auto id : flowIds) {
        flows.push_back(getFlow(id));
    }
}

bool TaskFlowManager::loadSubGraph(const string& graphName, const string& graphFileName, const KeyValueMap& kvMap)
{
    ScopedLock lock(_lock);
    GraphBuilder builder(_flowFactory, _rootPath, graphName);
    return builder.init(kvMap, graphFileName);
}

TaskFlowPtr TaskFlowManager::loadFlow(const string& fileName, const KeyValueMap& flowParam, bool runFlowAtOnce)
{
    ScopedLock lock(_lock);
    BS_PREFIX_LOG(INFO, "begin load flow from file [%s]", fileName.c_str());
    TaskFlowPtr flow = _flowFactory->createFlow(_rootPath, fileName, flowParam);
    if (!flow) {
        return TaskFlowPtr();
    }
    if (runFlowAtOnce) {
        flow->makeActive();
    }
    return flow;
}

TaskFlowPtr TaskFlowManager::loadSimpleFlow(const string& kernalType, const string& taskId, const KeyValueMap& kvMap,
                                            bool runFlowAtOnce)
{
    ScopedLock lock(_lock);
    BS_PREFIX_LOG(INFO, "load simple flow for task [id:%s, type:%s]", taskId.c_str(), kernalType.c_str());

    TaskFlowPtr flow = _flowFactory->createSimpleFlow(kernalType, taskId, kvMap);
    if (!flow) {
        return TaskFlowPtr();
    }
    if (runFlowAtOnce) {
        flow->makeActive();
    }
    return flow;
}

void TaskFlowManager::stepRun()
{
    ScopedLock lock(_lock);
    vector<TaskFlowPtr> flows = _flowContainer->getAllFlow();
    for (auto& flow : flows) {
        flow->stepRun();
    }
}

TaskFlowPtr TaskFlowManager::getFlow(const string& flowId) { return _flowContainer->getFlow(flowId); }

bool TaskFlowManager::stopFlow(const string& flowId)
{
    ScopedLock lock(_lock);
    TaskFlowPtr flow = _flowContainer->getFlow(flowId);
    if (!flow) {
        BS_PREFIX_LOG(ERROR, "stop flow [%s] fail, not exist!", flowId.c_str());
        return false;
    }
    return flow->stopFlow();
}

bool TaskFlowManager::suspendFlow(const string& flowId)
{
    ScopedLock lock(_lock);
    TaskFlowPtr flow = _flowContainer->getFlow(flowId);
    if (!flow) {
        BS_PREFIX_LOG(ERROR, "suspend flow [%s] fail, not exist!", flowId.c_str());
        return false;
    }
    return flow->suspendFlow();
}

bool TaskFlowManager::removeFlow(const string& flowId, bool clearTask)
{
    ScopedLock lock(_lock);
    TaskFlowPtr flow = _flowContainer->getFlow(flowId);
    if (!flow) {
        BS_PREFIX_LOG(ERROR, "remove flow [%s] fail, not exist!", flowId.c_str());
        return false;
    }

    if (flow->getStatus() == TaskFlow::tf_init) {
        BS_PREFIX_LOG(INFO, "removeFlow [%s]", flowId.c_str());
        _flowContainer->removeFlow(flowId);
        return true;
    }

    if (!flow->isFlowStopped() && !flow->isFlowFinish() && !flow->isFlowSuspended() && !flow->hasFatalError()) {
        BS_PREFIX_LOG(ERROR, "target flow [%s] can not be remove with status [%s]!", flowId.c_str(),
                      TaskFlow::status2Str(flow->getStatus()).c_str());
        return false;
    }

    if (clearTask) {
        BS_PREFIX_LOG(INFO, "clear all tasks in flow [%s]", flowId.c_str());
        flow->clearTasks();
    }
    BS_PREFIX_LOG(INFO, "removeFlow [%s]", flowId.c_str());
    _flowContainer->removeFlow(flowId);
    return true;
}

void TaskFlowManager::declareTaskParameter(const string& flowId, const string& taskId, const KeyValueMap& kvMap)
{
    return _flowFactory->declareTaskParameter(flowId, taskId, kvMap);
}

const TaskResourceManagerPtr& TaskFlowManager::getTaskResourceManager() const
{
    return _flowFactory->getTaskResourceManager();
}

const TaskFactoryPtr& TaskFlowManager::getTaskFactory() const { return _flowFactory->getTaskFactory(); }

void TaskFlowManager::TEST_setTaskFactory(const TaskFactoryPtr& factory) { _flowFactory->TEST_setTaskFactory(factory); }

void TaskFlowManager::serialize(Jsonizable::JsonWrapper& json) const
{
    vector<TaskBasePtr> tasks = _taskContainer->getAllTask();
    TaskSerializer::serialize(json, tasks);

    vector<TaskFlowPtr> flows = _flowContainer->getAllFlow();
    FlowSerializer::serialize(json, flows);
    json.Jsonize(FLOW_FACTORY, *_flowFactory);
    // FlowFactory::FlowTaskParameterMap paramMap = _flowFactory->getFlowTaskParameterMap();
    // json.Jsonize(INFLOW_TASK_PARAM_STR, paramMap, paramMap);
}

bool TaskFlowManager::deserialize(Jsonizable::JsonWrapper& json)
{
    ScopedLock lock(_lock);
    FlowFactory::FlowTaskParameterMap flowParamMap;
    json.Jsonize(INFLOW_TASK_PARAM_STR, flowParamMap, flowParamMap);
    json.Jsonize(FLOW_FACTORY, *_flowFactory);
    _flowFactory->setFlowTaskParameterMap(flowParamMap);

    try {
        if (!deserializeTasks(json)) {
            BS_PREFIX_LOG(ERROR, "deserialize tasks fail!");
            return false;
        }
        // attention: deserializeFlows must call after deserializeTasks
        if (!deserializeFlows(json)) {
            BS_PREFIX_LOG(ERROR, "deserialize flows fail!");
            return false;
        }
    } catch (const ExceptionBase& e) {
        BS_PREFIX_LOG(ERROR, "catch exception: [%s]", e.what());
        return false;
    }
    return true;
}

string TaskFlowManager::serialize() const
{
    Jsonizable::JsonWrapper wrapper;
    serialize(wrapper);
    return ToString(ToJson(wrapper.GetMap()));
}

bool TaskFlowManager::deserialize(const string& str)
{
    Any any;
    ParseJson(str, any);
    if (!IsJsonMap(any)) {
        BS_PREFIX_LOG(ERROR, "invalid str : [%s]", str.c_str());
        return false;
    }
    Jsonizable::JsonWrapper wrapper(any);
    return deserialize(wrapper);
}

bool TaskFlowManager::deserializeTasks(Jsonizable::JsonWrapper& json)
{
    TaskSerializer ts(_flowFactory->getTaskFactory(), _flowFactory->getTaskResourceManager());
    vector<TaskBasePtr> tasks;
    if (!ts.deserialize(json, tasks)) {
        BS_PREFIX_LOG(ERROR, "deserialize tasks fail!");
        return false;
    }
    _taskContainer->reset();
    for (auto& task : tasks) {
        if (!_taskContainer->addTask(task)) {
            return false;
        }
    }
    return true;
}

bool TaskFlowManager::deserializeFlows(Jsonizable::JsonWrapper& json)
{
    FlowSerializer fs(_rootPath, _flowFactory->getTaskFactory(), _taskContainer,
                      _flowFactory->getTaskResourceManager());

    vector<TaskFlowPtr> flows;
    if (!fs.deserialize(json, flows)) {
        BS_PREFIX_LOG(ERROR, "deserialize flows fail!");
        return false;
    }
    _flowContainer->reset();
    for (auto& flow : flows) {
        if (!_flowContainer->addFlow(flow)) {
            return false;
        }
    }
    return true;
}

vector<TaskBasePtr> TaskFlowManager::getAllTask() const { return _taskContainer->getAllTask(); }

vector<TaskFlowPtr> TaskFlowManager::getAllFlow() const { return _flowContainer->getAllFlow(); }

void TaskFlowManager::setLogPrefix(const string& str)
{
    _logPrefix = str;
    if (_taskContainer) {
        _taskContainer->setLogPrefix(str);
    }
    if (_flowContainer) {
        _flowContainer->setLogPrefix(str);
    }
    if (_flowFactory) {
        _flowFactory->setLogPrefix(str);
    }
}

vector<TaskBasePtr> TaskFlowManager::getOrphanTasks() const
{
    vector<TaskBasePtr> orphanTasks;
    vector<TaskBasePtr> allTasks = _taskContainer->getAllTask();
    for (auto& task : allTasks) {
        string flowId;
        string taskId;
        TaskFlow::getOriginalFlowIdAndTaskId(task->getIdentifier(), flowId, taskId);
        if (_flowContainer->getFlow(flowId) != NULL) {
            continue;
        }
        orphanTasks.push_back(task);
    }
    return orphanTasks;
}

string TaskFlowManager::getDotString(bool fillTask) const
{
    string ret = "digraph G {\n";
    ret += "compound=true\n";
    vector<TaskFlowPtr> flows = getAllFlow();
    for (auto& flow : flows) {
        ret += flow->getFlowLableString(fillTask);
    }

    if (fillTask) {
        vector<TaskBasePtr> orphanTasks = getOrphanTasks();
        if (!orphanTasks.empty()) {
            ret += "subgraph cluster_orphanTasks {\n"
                   "label=\"orphanTasks\";\n"
                   "color=red;\n";
            for (const auto& task : orphanTasks) {
                ret += task->getTaskLabelString();
            }
            ret += "}\n";
        }
    }
    ret += "}";
    return ret;
}

string TaskFlowManager::getDotStringForMatchingFlows(const string& flowParamKey, const string& flowParamValue,
                                                     bool fillTask) const
{
    string ret = "digraph G {\n";
    ret += "compound=true\n";
    vector<TaskFlowPtr> flows = getAllFlow();
    for (auto& flow : flows) {
        auto params = flow->getFlowParameterMap();
        auto iter = params.find(flowParamKey);
        if (iter == params.end() || iter->second != flowParamValue) {
            continue;
        }
        ret += flow->getFlowLableString(fillTask);
    }

    if (fillTask) {
        vector<TaskBasePtr> orphanTasks = getOrphanTasks();
        if (!orphanTasks.empty()) {
            ret += "subgraph cluster_orphanTasks {\n"
                   "label=\"orphanTasks\";\n"
                   "color=red;\n";
            for (const auto& task : orphanTasks) {
                ret += task->getTaskLabelString();
            }
            ret += "}\n";
        }
    }
    ret += "}";
    return ret;
}

void TaskFlowManager::fillFlowSubGraphInfo(KeyValueMap& subGraphStringMap,
                                           std::map<std::string, KeyValueMap>& taskDetailInfoMap)
{
    vector<TaskFlowPtr> flows = getAllFlow();
    for (auto& flow : flows) {
        string subFlowGraphString;
        flow->fillInnerTaskInfo(subFlowGraphString, taskDetailInfoMap);
        subGraphStringMap[flow->getFlowId()] = subFlowGraphString;
    }
}

void TaskFlowManager::fillTaskFlowInfos(GenerationInfo* generationInfo, const WorkerTable& workerTable,
                                        const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    vector<TaskFlowPtr> allFlows = getAllFlow();
    string taskStatusStr = "";
    for (auto& flow : allFlows) {
        if (flow->isFlowSuspending()) {
            taskStatusStr = "suspending";
        } else if (flow->isFlowSuspended()) {
            if (taskStatusStr == "") {
                taskStatusStr = "suspended";
            }
        } else {
            taskStatusStr = "normal";
            break;
        }
    }
    generationInfo->set_taskstatus(taskStatusStr);
    // gs task
    vector<TaskBasePtr> activeTasks;
    vector<TaskBasePtr> stoppedTasks;
    getTasks(allFlows, activeTasks, stoppedTasks);
    fillTaskInfos(generationInfo->mutable_activetaskinfos(), activeTasks, workerTable, roleCounterMap);
    fillTaskInfos(generationInfo->mutable_stoppedtaskinfos(), stoppedTasks, workerTable, roleCounterMap);

    // fill FlowMeta
    vector<TaskBasePtr> orphanTasks = getOrphanTasks();
    fillTaskFlowMetaInfos(allFlows, orphanTasks, generationInfo);
}

void TaskFlowManager::getTasks(const vector<TaskFlowPtr>& flows, vector<TaskBasePtr>& activeTasks,
                               vector<TaskBasePtr>& stopedTasks) const
{
    for (auto& flow : flows) {
        auto tasks = flow->getTasks();
        if (flow->isFlowFinish() || flow->isFlowStopped()) {
            stopedTasks.insert(stopedTasks.end(), tasks.begin(), tasks.end());
            continue;
        }
        activeTasks.insert(activeTasks.end(), tasks.begin(), tasks.end());
    }
}

TaskMaintainerPtr TaskFlowManager::getTaskMaintainer(const TaskFlowPtr& flow) const
{
    TaskMaintainerPtr taskMaintainer;
    if (!flow->isSimpleFlow()) {
        return taskMaintainer;
    }
    vector<TaskBasePtr> tasks = flow->getTasks();
    if (tasks.empty()) {
        return taskMaintainer;
    }
    assert(tasks.size() == 1);
    BuildServiceTaskPtr bsTask = DYNAMIC_POINTER_CAST(BuildServiceTask, tasks[0]);
    if (!bsTask) {
        return taskMaintainer;
    }

    taskMaintainer = DYNAMIC_POINTER_CAST(TaskMaintainer, bsTask->getTaskImpl());
    return taskMaintainer;
}

void TaskFlowManager::fillTaskInfos(::google::protobuf::RepeatedPtrField<proto::TaskInfo>* taskInfos,
                                    const vector<TaskBasePtr>& tasks, const WorkerTable& workerTable,
                                    const CounterCollector::RoleCounterMap& roleCounterMap) const
{
    for (auto& task : tasks) {
        BuildServiceTaskPtr bsTask = DYNAMIC_POINTER_CAST(BuildServiceTask, task);
        if (!bsTask) {
            continue;
        }
        TaskMaintainerPtr taskMaintainer = DYNAMIC_POINTER_CAST(TaskMaintainer, bsTask->getTaskImpl());
        if (!taskMaintainer) {
            continue;
        }
        proto::TaskInfo taskInfo;
        auto& nodes = workerTable.getWorkerNodes(task->getNodesIdentifier());
        TaskNodes taskNodes;
        taskNodes.reserve(nodes.size());
        for (const auto& workerNode : nodes) {
            TaskNodePtr node = DYNAMIC_POINTER_CAST(TaskNode, workerNode);
            if (node) {
                taskNodes.push_back(node);
            }
        }
        taskMaintainer->collectTaskInfo(&taskInfo, taskNodes, roleCounterMap);
        *taskInfos->Add() = taskInfo;
    }
}

void TaskFlowManager::fillTaskInfos(const vector<TaskBasePtr>& tasks,
                                    ::google::protobuf::RepeatedPtrField<proto::FlowTaskMeta>* taskMetas) const
{
    for (auto& task : tasks) {
        TaskBase::TaskMetaPtr meta = task->createTaskMeta();
        FlowTaskMeta* taskMeta = taskMetas->Add();
        taskMeta->set_taskid(meta->getTaskId());
        taskMeta->set_tasktype(meta->getTaskType());
        taskMeta->set_propertymapstr(ToJsonString(meta->getPropertyMap()));
    }
}

void TaskFlowManager::fillTaskFlowMetaInfos(const vector<TaskFlowPtr>& allFlows, const vector<TaskBasePtr>& orphanTasks,
                                            GenerationInfo* generationInfo) const
{
    vector<TaskFlowPtr> activeFlow;
    vector<TaskFlowPtr> stopFlow;
    for (auto& flow : allFlows) {
        if (flow->isFlowStopped() || flow->isFlowFinish()) {
            stopFlow.push_back(flow);
        } else {
            activeFlow.push_back(flow);
        }
    }

    fillFlowMetaInfos(activeFlow, generationInfo->mutable_activeflowmetas());
    fillFlowMetaInfos(stopFlow, generationInfo->mutable_stoppedflowmetas());
    fillTaskInfos(orphanTasks, generationInfo->mutable_orphantaskinfos());
}

void TaskFlowManager::fillFlowMetaInfos(const vector<TaskFlowPtr>& flows,
                                        ::google::protobuf::RepeatedPtrField<proto::FlowMeta>* flowMetas) const
{
    for (auto& flow : flows) {
        FlowMeta* flowMeta = flowMetas->Add();
        flowMeta->set_flowid(flow->getFlowId());
        flowMeta->set_graphid(flow->getGraphId());
        flowMeta->set_scriptinfo(flow->getScriptInfo());
        flowMeta->set_tags(flow->getTags());
        flowMeta->set_flowstatus(TaskFlow::status2Str(flow->getStatus()));
        flowMeta->set_errormsg(flow->getError());
        flowMeta->set_hasfatalerror(flow->hasFatalError());

        vector<string> upstreamInfos = flow->getUpstreamItemInfos();
        for (auto& str : upstreamInfos) {
            *flowMeta->add_upstreamflowinfos() = str;
        }
        fillTaskInfos(flow->getTasks(), flowMeta->mutable_inflowtaskmetas());
    }
}

#undef LOG_PREFIX

}} // namespace build_service::admin
