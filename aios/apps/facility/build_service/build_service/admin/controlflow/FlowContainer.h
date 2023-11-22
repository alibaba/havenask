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
#pragma once

#include <map>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class FlowContainer
{
public:
    FlowContainer();
    ~FlowContainer();

public:
    TaskFlowPtr getFlow(const std::string& flowId) const;
    void removeFlow(const std::string& flowId);
    bool addFlow(const TaskFlowPtr& flow);

    std::vector<TaskFlowPtr> getAllFlow() const;
    std::vector<TaskFlowPtr> getFlowsInGraph(const std::string& graphId) const;
    void getFlowIdByTag(const std::string& tag, std::vector<std::string>& flowIds) const;
    void reset();

    void setLogPrefix(const std::string& str) { _logPrefix = str; }

public:
    typedef std::map<std::string, TaskFlowPtr> FlowMap;
    FlowMap _flowMap;
    mutable autil::RecursiveThreadMutex _lock;
    std::string _logPrefix;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FlowContainer);

}} // namespace build_service::admin
