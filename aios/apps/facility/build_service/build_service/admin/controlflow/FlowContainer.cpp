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
#include "build_service/admin/controlflow/FlowContainer.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, FlowContainer);

#define LOG_PREFIX _logPrefix.c_str()

FlowContainer::FlowContainer() {}

FlowContainer::~FlowContainer() {}

TaskFlowPtr FlowContainer::getFlow(const string& flowId) const
{
    ScopedLock lock(_lock);
    FlowMap::const_iterator iter = _flowMap.find(flowId);
    if (iter != _flowMap.end()) {
        return iter->second;
    }
    return TaskFlowPtr();
}

void FlowContainer::getFlowIdByTag(const std::string& tag, std::vector<std::string>& flowIds) const
{
    ScopedLock lock(_lock);
    flowIds.clear();
    for (auto iter = _flowMap.begin(); iter != _flowMap.end(); iter++) {
        if (tag.empty() || iter->second->hasTag(tag)) {
            flowIds.push_back(iter->second->getFlowId());
        }
    }
}

void FlowContainer::removeFlow(const string& flowId)
{
    ScopedLock lock(_lock);
    TaskFlowPtr flow = getFlow(flowId);
    if (!flow) {
        BS_PREFIX_LOG(INFO, "remove flow [id:%s], flow not exist", flowId.c_str());
        return;
    }

    BS_PREFIX_LOG(INFO, "clear all tasks in flow [id:%s]!", flowId.c_str());
    flow->clearTasks();
    flow->setFlowContainer(NULL);
    BS_PREFIX_LOG(INFO, "remove flow [id:%s]", flowId.c_str());
    _flowMap.erase(flowId);
}

bool FlowContainer::addFlow(const TaskFlowPtr& taskFlow)
{
    if (!taskFlow) {
        return false;
    }
    ScopedLock lock(_lock);
    auto ret = _flowMap.insert(make_pair(taskFlow->getFlowId(), taskFlow));
    if (!ret.second) {
        BS_PREFIX_LOG(ERROR, "duplicate taskFlow with same id [%s]", taskFlow->getFlowId().c_str());
        return false;
    }

    taskFlow->setFlowContainer(this);
    BS_PREFIX_LOG(INFO, "add new flow [id:%s, tags:(%s)]", taskFlow->getFlowId().c_str(), taskFlow->getTags().c_str());
    return true;
}

vector<TaskFlowPtr> FlowContainer::getAllFlow() const
{
    ScopedLock lock(_lock);
    vector<TaskFlowPtr> ret;
    ret.reserve(_flowMap.size());

    FlowMap::const_iterator iter = _flowMap.begin();
    for (; iter != _flowMap.end(); iter++) {
        ret.push_back(iter->second);
    }
    return ret;
}

vector<TaskFlowPtr> FlowContainer::getFlowsInGraph(const string& graphId) const
{
    ScopedLock lock(_lock);
    vector<TaskFlowPtr> ret;
    ret.reserve(_flowMap.size());

    FlowMap::const_iterator iter = _flowMap.begin();
    for (; iter != _flowMap.end(); iter++) {
        if (iter->second->getGraphId() == graphId) {
            ret.push_back(iter->second);
        }
    }
    return ret;
}

void FlowContainer::reset()
{
    ScopedLock lock(_lock);
    _flowMap.clear();
}

#undef LOG_PREFIX

}} // namespace build_service::admin
