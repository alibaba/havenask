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

#include <stddef.h>
#include <string>
#include <vector>

#include "build_service/admin/controlflow/GraphBuilder.h"
#include "build_service/admin/controlflow/TaskFlowWrapper.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

extern "C" {
#include "lua.h"
}

#include "build_service/admin/controlflow/GraphBuilder.h"
#include "build_service/admin/controlflow/TaskFlowWrapper.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class GraphBuilderWrapper
{
public:
    GraphBuilderWrapper();
    ~GraphBuilderWrapper();

public:
    void setGraphBuilder(GraphBuilder* graphBuilder) { _graphBuilder = graphBuilder; }

public:
    TaskFlowWrapper loadFlow(const char* fileName, const char* flowParamStr = NULL);
    TaskFlowWrapper loadSimpleFlow(const char* kernelType, const char* taskId, const char* kvParamStr = NULL);
    bool loadSubGraph(const char* graphId, const char* graphFileName, const char* kvParamStr = NULL);

    TaskFlowWrapper getFlow(const char* flowId);
    void removeFlow(const char* flowId);
    const char* getFlowIdByTag(const char* tag);
    bool openApi(const char* cmd, const char* path, const char* params);

private:
    TaskFlowWrapper loadSimpleFlowWithKvParam(const char* kernelType, const char* taskId, const KeyValueMap& kvMap);

    bool loadSubGraphWithKvParam(const char* graphId, const char* graphFileName, const KeyValueMap& kvMap);

public:
    static void bindLua(lua_State* state);

private:
    GraphBuilder* _graphBuilder;
    std::vector<std::string> _resultFlowIds;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GraphBuilderWrapper);

}} // namespace build_service::admin
