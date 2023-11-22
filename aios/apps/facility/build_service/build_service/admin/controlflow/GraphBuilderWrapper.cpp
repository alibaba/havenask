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
#include "build_service/admin/controlflow/GraphBuilderWrapper.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>

#include "build_service/admin/controlflow/Eluna.h"
#include "build_service/admin/controlflow/GraphBuilder.h"
#include "build_service/admin/controlflow/KeyValueParamParser.h"
#include "build_service/admin/controlflow/ListParamParser.h"
#include "build_service/admin/controlflow/TaskFlow.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, GraphBuilderWrapper);

GraphBuilderWrapper::GraphBuilderWrapper() : _graphBuilder(NULL) {}

GraphBuilderWrapper::~GraphBuilderWrapper() {}

TaskFlowWrapper GraphBuilderWrapper::loadFlow(const char* fileName, const char* flowParamStr)
{
    KeyValueMap kvMap;
    if (!KeyValueParamParser::parseFromJsonString(flowParamStr, kvMap)) {
        return TaskFlowWrapper();
    }

    TaskFlowWrapper ret;
    if (_graphBuilder) {
        ret.setTaskFlow(_graphBuilder->loadFlow(fileName, kvMap).get());
    }
    return ret;
}

TaskFlowWrapper GraphBuilderWrapper::loadSimpleFlow(const char* kernelType, const char* taskId, const char* kvParamStr)
{
    KeyValueMap kvMap;
    if (!KeyValueParamParser::parseFromJsonString(kvParamStr, kvMap)) {
        return TaskFlowWrapper();
    }
    return loadSimpleFlowWithKvParam(kernelType, taskId, kvMap);
}

const char* GraphBuilderWrapper::getFlowIdByTag(const char* tag)
{
    vector<string> flowIds;
    if (_graphBuilder) {
        _graphBuilder->getFlowIdByTag(tag, flowIds);
    }
    ListParamParser parser;
    for (size_t i = 0; i < flowIds.size(); i++) {
        parser.pushBack(flowIds[i]);
    }
    string listStr = parser.toJsonString();
    _resultFlowIds.push_back(listStr);
    return _resultFlowIds.rbegin()->c_str();
}

TaskFlowWrapper GraphBuilderWrapper::loadSimpleFlowWithKvParam(const char* kernelType, const char* taskId,
                                                               const KeyValueMap& kvMap)
{
    TaskFlowWrapper ret;
    if (_graphBuilder) {
        ret.setTaskFlow(_graphBuilder->loadSimpleFlow(kernelType, taskId, kvMap).get());
    }
    return ret;
}

TaskFlowWrapper GraphBuilderWrapper::getFlow(const char* flowId)
{
    TaskFlowWrapper ret;
    if (_graphBuilder) {
        ret.setTaskFlow(_graphBuilder->getFlow(flowId).get());
    }
    return ret;
}

void GraphBuilderWrapper::removeFlow(const char* flowId)
{
    if (_graphBuilder) {
        _graphBuilder->removeFlow(flowId);
    }
}

bool GraphBuilderWrapper::loadSubGraph(const char* graphId, const char* graphFileName, const char* kvParamStr)
{
    KeyValueMap kvMap;
    if (!KeyValueParamParser::parseFromJsonString(kvParamStr, kvMap)) {
        return false;
    }
    return loadSubGraphWithKvParam(graphId, graphFileName, kvMap);
}

bool GraphBuilderWrapper::loadSubGraphWithKvParam(const char* graphId, const char* graphFileName,
                                                  const KeyValueMap& kvMap)
{
    if (_graphBuilder) {
        return _graphBuilder->loadSubGraph(graphId, graphFileName, kvMap);
    }
    return false;
}

bool GraphBuilderWrapper::openApi(const char* cmd, const char* path, const char* params)
{
    KeyValueMap kvMap;
    if (!KeyValueParamParser::parseFromJsonString(params, kvMap)) {
        return false;
    }
    if (_graphBuilder) {
        return _graphBuilder->openApi(cmd, path, kvMap);
    }
    return false;
}

void GraphBuilderWrapper::bindLua(lua_State* state)
{
    assert(state);
    ELuna::registerClass<GraphBuilderWrapper>(state, "GraphBuilderWrapper", ELuna::constructor<GraphBuilderWrapper>);
    ELuna::registerMethod<GraphBuilderWrapper>(state, "loadFlow", &GraphBuilderWrapper::loadFlow);
    ELuna::registerMethod<GraphBuilderWrapper>(state, "loadSimpleFlow", &GraphBuilderWrapper::loadSimpleFlow);
    ELuna::registerMethod<GraphBuilderWrapper>(state, "loadSubGraph", &GraphBuilderWrapper::loadSubGraph);
    ELuna::registerMethod<GraphBuilderWrapper>(state, "getFlow", &GraphBuilderWrapper::getFlow);
    ELuna::registerMethod<GraphBuilderWrapper>(state, "removeFlow", &GraphBuilderWrapper::removeFlow);
    ELuna::registerMethod<GraphBuilderWrapper>(state, "getFlowIdByTag", &GraphBuilderWrapper::getFlowIdByTag);
    ELuna::registerMethod<GraphBuilderWrapper>(state, "openApi", &GraphBuilderWrapper::openApi);
}

}} // namespace build_service::admin
