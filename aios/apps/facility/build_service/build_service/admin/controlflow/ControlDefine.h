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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

// bs tag
static const std::string FULL_PROCESS_TAG = "BSFullProcess";
static const std::string INC_PROCESS_TAG = "BSIncProcess";
static const std::string FULL_BUILD_TAG = "BSFullBuild";
static const std::string ROLLING_BACK_TAG = "BSRollback";

// for task
static const std::string TASK_META_ID = "id";
static const std::string TASK_META_TYPE = "type";
static const std::string TASK_META_PROPERTY = "property";

static const std::string TASK_META_STR = "task_meta";
static const std::string TASK_INFO_STR = "task_info";

// for flow
static const std::string TASK_FLOW_UPSTREAM_ID = "flow_id";
static const std::string TASK_FLOW_UPSTREAM_WAIT_STATUS = "wait_status";
static const std::string TASK_FLOW_UPSTREAM_MISSING_ACTION = "missing_action";

static const std::string SIMPLE_FLOW_INFO_PREFIX = "_SIMPLE_FLOW";
static const std::string FILE_FLOW_INFO_PREFIX = "_FILE_";

static const std::string FLOW_ID_STRING = "flowId";
static const std::string FLOW_IS_PUBLIC_STRING = "is_public";
static const std::string FLOW_HAS_FATAL_ERROR = "has_fatal_error";
static const std::string FLOW_SCRIPT_INFO_STR = "script_info";
static const std::string FLOW_GRAPH_ID_STR = "graph_id";
static const std::string FLOW_TAG_STR = "flow_tag";
static const std::string FLOW_TASK_PARAM_STR = "task_params";
static const std::string FLOW_GLOBAL_PARAM_STR = "global_params";
static const std::string FLOW_UPSTREAM_INFO_STR = "upstream_info";
static const std::string FLOW_FRIEND_STR = "friend_flows";
static const std::string FLOW_STATUS_STR = "status";
static const std::string FLOW_LAST_STATUS_STR = "last_status";
static const std::string FLOW_TASKID_SET_STR = "task_ids";
static const std::string FLOW_ERROR_MSG_STR = "error_msg";
static const std::string FLOW_PROPERTY_STR = "property";
static const std::string FLOW_PARAM_STR = "flow_params";

// for manager
static const std::string FLOW_ITEM_STR = "task_flows";
static const std::string TASK_ITEM_STR = "task_items";
static const std::string FLOW_FACTORY = "flow_factory";
static const std::string INFLOW_TASK_PARAM_STR = "task_parameters_in_flow";
static const std::string MAX_FLOW_ID = "max_flow_id";

////////////////// built-in SIMPL_FLOW_TEMPLATE script
static const std::string SIMPLE_FLOW_TEMPLATE = R"(
function stepRunFlow() 
    local task = Flow.createTask("%TASK_NAME%", "%TASK_TYPE%")
    if (not task:isValid())
    then
        Flow.setFatalError("createSimpleFlow Task id:%TASK_NAME%, type:%TASK_TYPE% fail!")
        return false
    end
    if (not Flow.startTask("%TASK_NAME%", Flow.parameter))
    then
        Flow.setFatalError("start SimpleFlow Task id:%TASK_NAME% fail!")
        return false
    end
    return true
end
)";

//////////////////  built-in Flow
static const std::string FLOW_SCRIPT_INTERFACE_STR = R"(
--define global params
_globalParam_ = {}
_logMeta_ = {}

--define current taskFlow object
_taskFlow_ = TaskFlowWrapper()

function createTask(taskId, kernalType, param)
    if (param == nil) then
        return _taskFlow_:createTask(taskId, kernalType, "{}")
    end

    if (type(param) == "table") then
        local kvMap = KVParamUtil()
        for k,v in pairs(param) do
            kvMap:setValue(k, v)
        end
        return _taskFlow_:createTask(taskId, kernalType, kvMap:toJsonString())
    end
    return _taskFlow_:createTask(taskId, kernalType, param)
end


function getTaskInFlow(taskId, flowId)
    return _taskFlow_:getTaskInFlow(taskId, flowId)
end

function getTask(taskId)
    return _taskFlow_:getTask(taskId)
end

function getFlowProperty(flowId, key)
    return _taskFlow_:getFlowProperty(flowId, key)
end

function getFlowStatus(flowId)
    return _taskFlow_:getFlowStatus(flowId)
end

function getTaskProperty(taskId, key, flowId)
    if (flowId == nil) then
        local task = _taskFlow_:getTask(taskId)
        return task:getProperty(key)
    end

    local task = getTaskInFlow(taskId, flowId)
    if (not task:isValid()) then
        return nil
    end
    return task:getProperty(key)
end

function setError(msg)
    _taskFlow_:setError(msg, false)
end

function setFatalError(msg)
    _taskFlow_:setError(msg, true)
end

function setProperty(key, value)
    _taskFlow_:setProperty(key, value)
end

function getProperty(key)
    return _taskFlow_:getProperty(key)
end

function suspendFlow()
    return _taskFlow_:suspendFlow()
end

function resumeFlow()
    return _taskFlow_:resumeFlow()
end

function stopFlow()
    return _taskFlow_:stopFlow()
end

function getFlowId()
    return _taskFlow_:getFlowId()
end

function startTask(taskItem, kvParam)
    local taskId = ""
    if (type(taskItem) == "string") then
        taskId = taskItem
    end

    local task = _taskFlow_:getTask(taskId)
    if (type(taskItem) == "userdata") then
        task = taskItem
    end

    if (not task:isValid()) then
        return false
    end

    if (kvParam == nil) then
        return task:start("{}")
    end

    if (type(kvParam) == "table") then
        local kvMap = KVParamUtil()
        for k,v in pairs(kvParam) do
            kvMap:setValue(k, v)
        end
        return task:start(kvMap:toJsonString())
    end
    return task:start(kvParam)
end

function openApi(cmd, path, param)
    if (type(param) == "table") then
        local kvMap = KVParamUtil()
        for k,v in pairs(param) do
            kvMap:setValue(k, v)
        end
        return _taskFlow_:openApi(cmd, path, kvMap:toJsonString())
    end
    return _taskFlow_:openApi(cmd, path, param)
end

function finishTask(taskItem, kvParam)
    local taskId = ""
    if (type(taskItem) == "string") then
        taskId = taskItem
    end
    local task = _taskFlow_:getTask(taskId)
    if (type(taskItem) == "userdata") then
        task = taskItem
    end
    if (not task:isValid()) then
        return false
    end
    if (kvParam == nil) then
        return task:finish("{}")
    end
    if (type(kvParam) == "table") then
        local kvMap = KVParamUtil()
        for k,v in pairs(kvParam) do
            kvMap:setValue(k, v)
        end
        return task:finish(kvMap:toJsonString())
    end
    return task:finish(kvParam)
end

-- interface define --
Flow = {}
Flow.parameter = _globalParam_
Flow.createTask = createTask
Flow.getTaskProperty = getTaskProperty
Flow.getTaskInFlow = getTaskInFlow
Flow.getFlowProperty = getFlowProperty
Flow.getFlowStatus = getFlowStatus
Flow.setError = setError
Flow.setFatalError = setFatalError
Flow.setProperty = setProperty
Flow.getProperty = getProperty
Flow.startTask = startTask
Flow.finishTask = finishTask
Flow.getTask = getTask
Flow.getFlowId = getFlowId
Flow.openApi = openApi
)";

//////////////////  built-in Graph
static const std::string GRAPH_SCRIPT_INTERFACE_STR = R"(
--define global params
_globalParam_ = {}
_logMeta_ = {}

--define global task flow manager
_graphBuilder_ = GraphBuilderWrapper()

function batchStopFlows(flowIds)
     for i, v in ipairs(flowIds)
     do
            local flow = _graphBuilder_:getFlow(v)
            flow:stopFlow()
            print("stopping flow:"..v)
      end
end

function batchSetUpstream(flowId, upstreamFlowIds, waitStatus)
     local flow = _graphBuilder_:getFlow(flowId)
     for i, v in ipairs(upstreamFlowIds)
     do
            local upstreamFlow = _graphBuilder_:getFlow(v)
            if (waitStatus == nil) then
                flow:addUpstreamFlow(upstreamFlow, "finish", "stop")
            else
                flow:addUpstreamFlow(upstreamFlow, waitStatus, "stop")
            end
      end
end


function getFlowIdByTag(tag)
    if (tag == nil) then
        return nil
    end
    local flowIdStr = _graphBuilder_:getFlowIdByTag(tag)
    local list = {}
    local wrapper = ListParamUtil()
    if (wrapper:fromJsonString(flowIdStr)) then
        local size = wrapper:getSize()
        for i=1,size do
            local value = wrapper:getValue(i - 1)
            list[i] = value
        end
    end
    return list
end

function loadFlow(fileName, flowKVParam)
    if (flowKVParam == nil) then
        return _graphBuilder_:loadFlow(fileName, "{}")
    end

    if (type(flowKVParam) == "table") then
        local kvMap = KVParamUtil()
        for k,v in pairs(flowKVParam) do
            kvMap:setValue(k, v)
        end
        return _graphBuilder_:loadFlow(fileName, kvMap:toJsonString())
    end
    return _graphBuilder_:loadFlow(fileName, flowKVParam)
end

function loadSimpleFlow(kernalType, taskId, kvParam)
    if (kvParam == nil) then
        return _graphBuilder_:loadSimpleFlow(kernalType, taskId, "{}")
    end

    if (type(kvParam) == "table") then
        local kvMap = KVParamUtil()
        for k,v in pairs(kvParam) do
            kvMap:setValue(k, v)
        end
        return _graphBuilder_:loadSimpleFlow(kernalType, taskId, kvMap:toJsonString())
    end
    return _graphBuilder_:loadSimpleFlow(kernalType, taskId, kvParam)
end

function loadSubGraph(graphId, graphFileName, kvParam)
    if (kvParam == nil) then
        return _graphBuilder_:loadSubGraph(graphId, graphFileName, "{}")
    end
    if (type(kvParam) == "table") then
        local kvMap = KVParamUtil()
        for k,v in pairs(kvParam) do
            kvMap:setValue(k, v)
        end
        return _graphBuilder_:loadSubGraph(graphId, graphFileName, kvMap:toJsonString())
    end
    return _graphBuilder_:loadSubGraph(graphId, graphFileName, kvParam)
end

function getFlow(flowId)
    return _graphBuilder_:getFlow(flowId)
end

function setUpstream(flow, upstreamFlow, waitStatus)
    if (waitStatus == nil) then
        flow:addUpstreamFlow(upstreamFlow, "finish", "stop")
    else
        flow:addUpstreamFlow(upstreamFlow, waitStatus, "stop")
    end
end

function declareFriend(flow, friendFlow)
    if (type(friendFlow) == "string") then
        return flow:addFriendFlowId(friendFlow)
    end
    return flow:addFriendFlowId(friendFlow:getFlowId())
end

function openApi(cmd, path, param)
    if (type(param) == "table") then
        local kvMap = KVParamUtil()
        for k,v in pairs(param) do
            kvMap:setValue(k, v)
        end
        return _graphBuilder_:openApi(cmd, path, kvMap:toJsonString())
    end
    return _graphBuilder_:openApi(cmd, path, param)
end

-- interface define --
Graph = {}
Graph.parameter = _globalParam_
Graph.loadFlow = loadFlow
Graph.loadSimpleFlow = loadSimpleFlow
Graph.loadSubGraph = loadSubGraph
Graph.getFlow = getFlow
Graph.setUpstream = setUpstream
Graph.declareFriend = declareFriend
Graph.getFlowIdByTag = getFlowIdByTag
Graph.batchStopFlows = batchStopFlows
Graph.batchSetUpstream = batchSetUpstream
Graph.openApi = openApi
)";

/////////////////////// built-in Tool
static const std::string TOOL_SCRIPT_INTERFACE_STR = R"(
-- define list function
function listToJson(list)
    local wrapper = ListParamUtil()
    for i,v in ipairs(list) do 
        wrapper:pushBack(v)
    end
    local ret = wrapper:toJsonString()
    return ret
end

function jsonToList(str)
    local list = {}
    local wrapper = ListParamUtil()
    if (wrapper:fromJsonString(str)) then
        local size = wrapper:getSize()
        for i=1,size do
            local value = wrapper:getValue(i - 1)
            list[i] = value
        end
    end
    return list
end

function listToStr(list, delim)
    local wrapper = ListParamUtil()
    for i,v in ipairs(list) do 
        wrapper:pushBack(v)
    end
    local ret = wrapper:toListString(delim)
    return ret
end

function strToList(str, delim)
    local list = {}
    local wrapper = ListParamUtil()
    if (wrapper:fromListString(str, delim)) then
        local size = wrapper:getSize()
        for i=1,size do
            local value = wrapper:getValue(i - 1)
            list[i] = value
        end
    end
    return list
end

function copyList(list)
    local str = listToJson(list)
    return jsonToList(str)
end

-- define map function
function mapToJson(map)
    local wrapper = KVParamUtil()
    for k,v in pairs(map) do
        wrapper:setValue(k, v)
    end
    local ret = wrapper:toJsonString()
    return ret
end

function jsonToMap(str)
    local map = {}
    local wrapper = KVParamUtil()
    if (wrapper:fromJsonString(str)) then
        local size = wrapper:getKeySize()
        for i=1,size do
            local key = wrapper:getKey(i - 1)
            map[key] = wrapper:getValue(key)
        end
    end
    return map
end

function mapToStr(map, kvDelim, pairDelim)
    local wrapper = KVParamUtil()
    for k,v in pairs(map) do
        wrapper:setValue(k, v)
    end
    local ret = wrapper:toKVPairString(kvDelim, pairDelim)
    return ret
end

function strToMap(str, kvDelim, pairDelim)
    local map = {}
    local wrapper = KVParamUtil()
    if (wrapper:fromKVPairString(str, kvDelim, pairDelim)) then
        local size = wrapper:getKeySize()
        for i=1,size do
            local key = wrapper:getKey(i - 1)
            map[key] = wrapper:getValue(key)
        end
    end
    return map
end

function copyMap(map)
    local str = mapToJson(map)
    return jsonToMap(str)
end

function getJsonStringValue(str, fieldName)
    local wrapper = JsonParamUtil()
    if (wrapper:fromJsonString(str)) then
        return wrapper:getStringValue(fieldName)
    end
    return ''
end

-- interface define --
Tool = {}
Tool.listToJson = listToJson
Tool.jsonToList = jsonToList
Tool.listToStr = listToStr
Tool.strToList = strToList
Tool.copyList = copyList
Tool.mapToJson = mapToJson
Tool.jsonToMap = jsonToMap
Tool.mapToStr = mapToStr
Tool.strToMap = strToMap
Tool.copyMap = copyMap
Tool.getJsonStringValue = getJsonStringValue

)";

static const std::string IMPORT_TOOL_STR = "#import(Tool)";
static const std::string COMMENT_IMPORT_TOOL_STR = "--#import(Tool)";

/////////////////////// built-in Log
static const std::string LOG_SCRIPT_INTERFACE_STR = R"(
--define logger object
_loggerTable_ = {}

function innerLog(lineNumKey, level, msg, interval)
    local logger = _loggerTable_[lineNumKey]
    if (logger ~= nil) then
        if (interval == nil) then
            logger:log(level, msg)
        else
            logger:interval_log(level, msg, interval)
        end
        return
    end

    local newLogger = LuaLogger()
    local logPrefix = ""
    if (_logMeta_["prefix"] ~= nil) then
        logPrefix = _logMeta_["prefix"]
    end
    newLogger:setPrefix(logPrefix)

    local fileName = ""
    if (_logMeta_["fileName"] ~= nil) then
        fileName = _logMeta_["fileName"]
    end

    local hostId = ""
    if (_logMeta_["hostId"] ~= nil) then
        hostId = _logMeta_["hostId"]
    end

    newLogger:setLocation(hostId, fileName, lineNumKey)
    _loggerTable_[lineNumKey] = newLogger

    if (interval == nil) then
        newLogger:log(level, msg)
    else
        newLogger:interval_log(level, msg, interval)
    end
end

--script usage: Log.log(level, msg)
Log = {}
Log.innerLog = innerLog
)";

static const std::string IMPORT_LOG_STR = "#import(Log)";
static const std::string COMMENT_IMPORT_LOG_STR = "--#import(Log)";
static const std::string CALL_LOG_PREFIX = "Log.log(";
static const std::string REPLACE_LOG_PREFIX = "Log.innerLog(";

inline bool enableTool(const std::string& str)
{
    return str.find(IMPORT_TOOL_STR) != std::string::npos && str.find(COMMENT_IMPORT_TOOL_STR) == std::string::npos;
}

inline bool enableLog(const std::string& str)
{
    return str.find(IMPORT_LOG_STR) != std::string::npos && str.find(COMMENT_IMPORT_LOG_STR) == std::string::npos;
}

}} // namespace build_service::admin
