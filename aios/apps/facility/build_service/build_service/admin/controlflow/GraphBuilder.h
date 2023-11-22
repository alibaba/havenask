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

#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/common_define.h"

BS_DECLARE_REFERENCE_CLASS(admin, FlowFactory);
#include <string>
#include <vector>

extern "C" {
#include "lua.h"
}

#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"

BS_DECLARE_REFERENCE_CLASS(admin, FlowFactory);

namespace build_service { namespace admin {

class GraphBuilder
{
public:
    GraphBuilder(const FlowFactoryPtr& flowFactory, const std::string& rootPath, const std::string& graphName);
    ~GraphBuilder();

public:
    bool init(const KeyValueMap& kvMap, const std::string& graphFileName);

    TaskFlowPtr loadFlow(const std::string& fileName, const KeyValueMap& flowMap = KeyValueMap());

    TaskFlowPtr loadSimpleFlow(const std::string& kernalType, const std::string& taskId,
                               const KeyValueMap& kvMap = KeyValueMap());

    bool loadSubGraph(const std::string& graphName, const std::string& graphFileName,
                      const KeyValueMap& kvMap = KeyValueMap());

    TaskFlowPtr getFlow(const std::string& flowId);
    void removeFlow(const std::string& flowId);
    void getFlowIdByTag(const std::string& tag, std::vector<std::string>& flowIds);
    bool openApi(const std::string& cmd, const std::string& path, const KeyValueMap& params);

private:
    void registerTaskClass(lua_State* state);
    bool initFlowByString(lua_State* state, const std::string& input);
    bool getFlowString(const std::string& input, std::string& flowStr);
    bool prepareLuaParam(lua_State* state, const std::string& graphFileName);

private:
    lua_State* _lState;
    FlowFactoryPtr _flowFactory;
    std::string _rootPath;
    std::string _graphName;
    KeyValueMap _globalParams;
    bool _createFlowError;
    std::vector<std::string> _flowIds;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GraphBuilder);

}} // namespace build_service::admin
