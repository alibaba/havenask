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
#include "build_service/admin/controlflow/FlowFactory.h"

#include <algorithm>
#include <iosfwd>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/controlflow/ControlDefine.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, FlowFactory);

#define LOG_PREFIX _logPrefix.c_str()

FlowFactory::FlowFactory(const FlowContainerPtr& flowContainer, const TaskContainerPtr& taskContainer,
                         const TaskFactoryPtr& factory, const TaskResourceManagerPtr& resMgr)
    : _flowContainer(flowContainer)
    , _taskContainer(taskContainer)
    , _taskResMgr(resMgr)
    , _factory(factory)
    , _maxFlowId(-1)
{
    if (!_taskResMgr) {
        _taskResMgr.reset(new TaskResourceManager);
    }
}

FlowFactory::~FlowFactory() {}

TaskFlowPtr FlowFactory::createFlow(const string& rootPath, const string& fileName, const KeyValueMap& flowParams)
{
    if (!_flowContainer || !_taskContainer || !_factory) {
        return TaskFlowPtr();
    }

    string flowId;
    {
        ScopedLock lock(_lock);
        flowId = StringUtil::toString(++_maxFlowId);
    }
    if (flowId.find(".") != string::npos) {
        BS_PREFIX_LOG(ERROR, "invalid flowId [%s], should not has \".\" in flowId", flowId.c_str());
        return TaskFlowPtr();
    }
    TaskFlowPtr taskFlow(new TaskFlow(rootPath, _factory, _taskContainer, _taskResMgr));
    if (!taskFlow->init(fileName, flowId, flowParams)) {
        return TaskFlowPtr();
    }

    ScopedLock lock(_lock);
    FlowTaskParameterMap::const_iterator iter = _flowKVMap.find(flowId);
    if (iter != _flowKVMap.end()) {
        BS_PREFIX_LOG(INFO, "start taskFlow [%s] with parameter [%s]", flowId.c_str(),
                      ToJsonString(iter->second, true).c_str());
        taskFlow->setTaskParameter(iter->second);
    }

    if (!_flowContainer->addFlow(taskFlow)) {
        return TaskFlowPtr();
    }
    return taskFlow;
}

TaskFlowPtr FlowFactory::createSimpleFlow(const string& kernalType, const string& taskId, const KeyValueMap& taskKVMap)
{
    if (!_flowContainer || !_taskContainer || !_factory) {
        return TaskFlowPtr();
    }

    string flowId;
    {
        ScopedLock lock(_lock);
        flowId = StringUtil::toString(++_maxFlowId);
    }

    TaskFlowPtr taskFlow(new TaskFlow("", _factory, _taskContainer, _taskResMgr));
    if (!taskFlow->initSimpleFlow(taskId, kernalType, taskKVMap, flowId)) {
        return TaskFlowPtr();
    }

    ScopedLock lock(_lock);
    FlowTaskParameterMap::const_iterator iter = _flowKVMap.find(flowId);
    if (iter != _flowKVMap.end()) {
        BS_PREFIX_LOG(INFO, "start taskFlow [%s] with parameter [%s]", flowId.c_str(),
                      ToJsonString(iter->second, true).c_str());
        taskFlow->setTaskParameter(iter->second);
    }

    if (!_flowContainer->addFlow(taskFlow)) {
        return TaskFlowPtr();
    }
    return taskFlow;
}

TaskFlowPtr FlowFactory::getFlow(const string& flowId) const { return _flowContainer->getFlow(flowId); }

void FlowFactory::removeFlow(const string& flowId) const { _flowContainer->removeFlow(flowId); }

void FlowFactory::declareTaskParameter(const string& flowId, const string& taskId, const KeyValueMap& kvMap)
{
    ScopedLock lock(_lock);
    TaskParameterMap& kv = _flowKVMap[flowId];
    kv[taskId] = kvMap;
    TaskFlowPtr flow = _flowContainer->getFlow(flowId);
    if (flow) {
        flow->setTaskParameter(kv);
    }
}

void FlowFactory::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    ScopedLock lock(_lock);
    json.Jsonize(INFLOW_TASK_PARAM_STR, _flowKVMap, _flowKVMap);
    json.Jsonize(MAX_FLOW_ID, _maxFlowId, _maxFlowId);
}

void FlowFactory::getFlowIdByTag(const std::string& tag, std::vector<std::string>& flowIds)
{
    ScopedLock lock(_lock);
    _flowContainer->getFlowIdByTag(tag, flowIds);
    sort(flowIds.begin(), flowIds.end(), flowIdCompare);
}

FlowFactory::FlowTaskParameterMap FlowFactory::getFlowTaskParameterMap() const
{
    ScopedLock lock(_lock);
    return _flowKVMap;
}

void FlowFactory::setFlowTaskParameterMap(const FlowTaskParameterMap& paramMap)
{
    ScopedLock lock(_lock);
    _flowKVMap = paramMap;
}

void FlowFactory::setLogPrefix(const string& str)
{
    _logPrefix = str;
    if (_taskResMgr) {
        _taskResMgr->setLogPrefix(str);
    }
}

#undef LOG_PREFIX

}} // namespace build_service::admin
