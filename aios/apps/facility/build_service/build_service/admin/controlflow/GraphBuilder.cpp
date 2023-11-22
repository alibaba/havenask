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
#include "build_service/admin/controlflow/GraphBuilder.h"

#include <algorithm>
#include <assert.h>
#include <map>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "build_service/admin/controlflow/ControlDefine.h"
#include "build_service/admin/controlflow/Eluna.h"
#include "build_service/admin/controlflow/FlowFactory.h"
#include "build_service/admin/controlflow/GraphBuilderWrapper.h"
#include "build_service/admin/controlflow/KeyValueParamParser.h"
#include "build_service/admin/controlflow/ListParamParser.h"
#include "build_service/admin/controlflow/LocalLuaScriptReader.h"
#include "build_service/admin/controlflow/LuaLoggerWrapper.h"
#include "build_service/admin/controlflow/OpenApiHandler.h"
#include "build_service/admin/controlflow/TaskFlowWrapper.h"
#include "build_service/util/ErrorLogCollector.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::util;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, GraphBuilder);

#undef LOG_PREFIX
#define LOG_PREFIX _flowFactory->getLogPrefix().c_str()

GraphBuilder::GraphBuilder(const FlowFactoryPtr& flowFactory, const string& rootPath, const string& graphName)
    : _lState(NULL)
    , _flowFactory(flowFactory)
    , _rootPath(rootPath)
    , _graphName(graphName)
    , _createFlowError(false)
{
}

GraphBuilder::~GraphBuilder()
{
    if (_lState) {
        ELuna::closeLua(_lState);
        _lState = NULL;
    }
}

bool GraphBuilder::init(const KeyValueMap& kvMap, const string& graphFileName)
{
    _globalParams = kvMap;

    string fileContent;
    string filePath;
    if (!LocalLuaScriptReader::readScriptFile(_flowFactory->getTaskResourceManager(), _rootPath, graphFileName,
                                              filePath, fileContent)) {
        BS_PREFIX_LOG(ERROR, "read file [%s] fail", filePath.c_str());
        return false;
    }

    BS_PREFIX_LOG(INFO, "load graph [%s] from [%s]", _graphName.c_str(), filePath.c_str());
    _lState = ELuna::openLua();
    if (_lState == NULL) {
        return false;
    }
    registerTaskClass(_lState);
    if (!initFlowByString(_lState, fileContent)) {
        BS_PREFIX_LOG(ERROR, "load flow_graph from file [%s] fail", filePath.c_str());
        return false;
    }
    if (!prepareLuaParam(_lState, graphFileName)) {
        return false;
    }

    ELuna::LuaFunction<bool> run(_lState, "graphDef");
    bool ret = run();
    if (run.hasError()) {
        BS_PREFIX_LOG(ERROR, "run flow roadmap file [%s] fail!", filePath.c_str());
        // rollback created flows
        for (auto& id : _flowIds) {
            _flowFactory->removeFlow(id);
        }
        return false;
    }

    if (!ret || _createFlowError) {
        if (!ret) {
            BS_PREFIX_LOG(ERROR, "run graphDef return false in graph file [%s] fail!", filePath.c_str());
        } else {
            BS_PREFIX_LOG(ERROR, "load flow fail in graph file [%s] fail!", filePath.c_str());
        }
        // rollback created flows
        for (auto& id : _flowIds) {
            _flowFactory->removeFlow(id);
        }
        return false;
    }
    BS_PREFIX_LOG(INFO, "init graph from [%s] success!", graphFileName.c_str());
    return true;
}

void GraphBuilder::registerTaskClass(lua_State* state)
{
    // TaskWrapper::bindLua(state);
    LuaLoggerWrapper::bindLua(state);
    ListParamWrapper::bindLua(state);
    KeyValueParamWrapper::bindLua(state);
    TaskFlowWrapper::bindLua(state);
    GraphBuilderWrapper::bindLua(state);
}

bool GraphBuilder::initFlowByString(lua_State* state, const string& input)
{
    assert(state);
    string flowStr;
    if (!getFlowString(input, flowStr)) {
        return false;
    }

    BS_PREFIX_LOG(DEBUG, "init control flow by string [%s]", flowStr.c_str());
    return ELuna::doString(state, flowStr.c_str());
}

void GraphBuilder::getFlowIdByTag(const std::string& tag, std::vector<std::string>& flowIds)
{
    _flowFactory->getFlowIdByTag(tag, flowIds);
}

bool GraphBuilder::getFlowString(const string& input, string& flowStr)
{
    if (input.find("function graphDef()") == string::npos) {
        BS_PREFIX_LOG(ERROR, "no graphDef function found! content [%s]", input.c_str());
        return false;
    }

    if (input.find("return true") == string::npos) {
        BS_PREFIX_LOG(ERROR, "graphDef should call return true in the end, content [%s]", input.c_str());
        return false;
    }

    string tmp = input;
    tmp = LuaLoggerWrapper::rewrite(input);
    // rewrite for import library
    stringstream ss;
    vector<string> lines;
    StringUtil::split(lines, tmp, "\n");
    ss << GRAPH_SCRIPT_INTERFACE_STR;
    for (size_t i = 0; i < lines.size(); i++) {
        if (!StringUtil::startsWith(lines[i], "#import(")) {
            ss << lines[i] << "\n";
            continue;
        }
        StringUtil::trim(lines[i]);
        string module = lines[i].substr(8, lines[i].size() - 9);
        if (module == "Tool") {
            ss << TOOL_SCRIPT_INTERFACE_STR << "\n";
            continue;
        }
        if (module == "Log") {
            continue;
        }
        string importFile = module + ".lua";
        string filePath, fileContent;
        if (!LocalLuaScriptReader::readScriptFile(_flowFactory->getTaskResourceManager(), _rootPath, importFile,
                                                  filePath, fileContent)) {
            BS_PREFIX_LOG(ERROR, "import [%s] fail", filePath.c_str());
            return false;
        }
        BS_PREFIX_LOG(INFO, "import [%s] success", filePath.c_str());
        ss << fileContent << "\n";
    }
    flowStr = ss.str();
    return true;
}

bool GraphBuilder::prepareLuaParam(lua_State* state, const string& graphFileName)
{
    lua_getglobal(state, "_graphBuilder_");
    if (!lua_isuserdata(state, -1)) {
        BS_PREFIX_LOG(ERROR, "_graphBuilder_ not define in roadmap lua script!");
        return false;
    }

    GraphBuilderWrapper& ref = ELuna::convert2CppType<GraphBuilderWrapper&>::convertType(state, lua_gettop(state));
    ref.setGraphBuilder(this);

    ELuna::LuaTable globalParamTable(state, "_globalParam_");
    KeyValueMap::const_iterator iter = _globalParams.begin();
    for (; iter != _globalParams.end(); iter++) {
        globalParamTable.set(iter->first, iter->second);
    }

    ELuna::LuaTable logMetaTable(state, "_logMeta_");
    logMetaTable.set("prefix", _flowFactory->getLogPrefix());
    logMetaTable.set("fileName", graphFileName);
    logMetaTable.set("hostId", _graphName);
    return true;
}

TaskFlowPtr GraphBuilder::loadFlow(const string& fileName, const KeyValueMap& flowParams)
{
    BS_PREFIX_LOG(INFO, "begin load flow in graph [%s] from file [%s]", _graphName.c_str(), fileName.c_str());

    TaskFlowPtr flow = _flowFactory->createFlow(_rootPath, fileName, flowParams);
    if (!flow) {
        _createFlowError = true;
        return TaskFlowPtr();
    }

    _flowIds.push_back(flow->getFlowId());
    flow->setGraphId(_graphName);
    flow->makeActive();
    return flow;
}

TaskFlowPtr GraphBuilder::loadSimpleFlow(const string& kernalType, const string& taskId, const KeyValueMap& kvMap)
{
    BS_PREFIX_LOG(INFO, "load simple flow in graph [%s] for task [id:%s, type:%s]", _graphName.c_str(), taskId.c_str(),
                  kernalType.c_str());

    TaskFlowPtr flow = _flowFactory->createSimpleFlow(kernalType, taskId, kvMap);
    if (!flow) {
        _createFlowError = true;
        return TaskFlowPtr();
    }

    _flowIds.push_back(flow->getFlowId());
    flow->setGraphId(_graphName);
    flow->makeActive();
    return flow;
}

TaskFlowPtr GraphBuilder::getFlow(const string& flowId) { return _flowFactory->getFlow(flowId); }

void GraphBuilder::removeFlow(const string& flowId) { _flowFactory->removeFlow(flowId); }

bool GraphBuilder::loadSubGraph(const string& graphName, const string& graphFileName, const KeyValueMap& kvMap)
{
    GraphBuilder builder(_flowFactory, _rootPath, graphName);
    return builder.init(kvMap, graphFileName);
}

bool GraphBuilder::openApi(const std::string& cmd, const std::string& path, const KeyValueMap& params)
{
    OpenApiHandler handler(_flowFactory->getTaskResourceManager());
    return handler.handle(cmd, path, params);
}

#undef LOG_PREFIX

}} // namespace build_service::admin
