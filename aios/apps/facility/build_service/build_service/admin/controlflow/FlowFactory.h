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
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/controlflow/FlowContainer.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"

namespace build_service { namespace admin {

class FlowFactory : public autil::legacy::Jsonizable
{
public:
    // key = taskId, value = KVMap
    typedef std::map<std::string, KeyValueMap> TaskParameterMap;

    // key = flowId, value = task Parametrer
    typedef std::map<std::string, TaskParameterMap> FlowTaskParameterMap;

public:
    FlowFactory(const FlowContainerPtr& flowContainer, const TaskContainerPtr& taskContainer,
                const TaskFactoryPtr& factory, const TaskResourceManagerPtr& resMgr);

    ~FlowFactory();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    TaskFlowPtr createFlow(const std::string& rootPath, const std::string& fileName, const KeyValueMap& flowParams);

    TaskFlowPtr createSimpleFlow(const std::string& kernalType, const std::string& taskId,
                                 const KeyValueMap& taskKVMap);

    TaskFlowPtr getFlow(const std::string& flowId) const;
    void removeFlow(const std::string& flowId) const;

    void declareTaskParameter(const std::string& flowId, const std::string& taskId, const KeyValueMap& kvMap);

    const TaskResourceManagerPtr& getTaskResourceManager() const { return _taskResMgr; }
    const TaskFactoryPtr& getTaskFactory() const { return _factory; }

    FlowTaskParameterMap getFlowTaskParameterMap() const;
    void setFlowTaskParameterMap(const FlowTaskParameterMap& paramMap);

    void setLogPrefix(const std::string& str);
    const std::string& getLogPrefix() const { return _logPrefix; }
    void getFlowIdByTag(const std::string& tag, std::vector<std::string>& flowIds);

public:
    void TEST_setTaskFactory(const TaskFactoryPtr& factory) { _factory = factory; }

private:
    static bool flowIdCompare(const std::string& left, const std::string& right)
    {
        int64_t leftNum;
        autil::StringUtil::fromString(left, leftNum);
        int64_t rightNum;
        autil::StringUtil::fromString(right, rightNum);
        return leftNum < rightNum;
    }

private:
    FlowContainerPtr _flowContainer;
    TaskContainerPtr _taskContainer;
    FlowTaskParameterMap _flowKVMap;
    TaskResourceManagerPtr _taskResMgr;
    mutable autil::RecursiveThreadMutex _lock;
    TaskFactoryPtr _factory;
    std::string _logPrefix;
    int64_t _maxFlowId;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FlowFactory);

}} // namespace build_service::admin
