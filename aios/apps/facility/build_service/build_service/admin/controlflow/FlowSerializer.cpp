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
#include "build_service/admin/controlflow/FlowSerializer.h"

#include "build_service/admin/controlflow/ControlDefine.h"
#include "build_service/admin/controlflow/TaskContainer.h"
#include "build_service/admin/controlflow/TaskFlow.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, FlowSerializer);

#undef LOG_PREFIX
#define LOG_PREFIX _taskContainer->getLogPrefix().c_str()

FlowSerializer::FlowSerializer(const string& rootPath, const TaskFactoryPtr& factory,
                               const TaskContainerPtr& taskContainer, const TaskResourceManagerPtr& taskResMgr)
    : _rootPath(rootPath)
    , _factory(factory)
    , _taskContainer(taskContainer)
    , _resMgr(taskResMgr)
{
}

FlowSerializer::~FlowSerializer() {}

void FlowSerializer::serialize(Jsonizable::JsonWrapper& json, vector<TaskFlowPtr>& taskFlows)
{
    json.Jsonize(FLOW_ITEM_STR, taskFlows);
}

bool FlowSerializer::deserialize(Jsonizable::JsonWrapper& json, vector<TaskFlowPtr>& taskFlows)
{
    JsonMap flowMap = json.GetMap();
    if (flowMap.find(FLOW_ITEM_STR) == flowMap.end()) {
        BS_PREFIX_LOG(ERROR, "no %s find!", FLOW_ITEM_STR.c_str());
        return false;
    }

    Any& any = flowMap[FLOW_ITEM_STR];
    if (!IsJsonArray(any)) {
        return false;
    }

    taskFlows.clear();
    JsonArray array = AnyCast<JsonArray>(any);
    taskFlows.reserve(array.size());
    for (auto& flowAny : array) {
        TaskFlowPtr newFlow(new TaskFlow(_rootPath, _factory, _taskContainer, _resMgr));
        FromJson(*newFlow, flowAny);
        taskFlows.push_back(newFlow);
    }
    return true;
}

#undef LOG_PREFIX

}} // namespace build_service::admin
