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
#include "build_service/admin/controlflow/TaskFlow.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/string_tools.h"
#include "build_service/admin/controlflow/ControlDefine.h"
#include "build_service/admin/controlflow/Eluna.h"
#include "build_service/admin/controlflow/FlowContainer.h"
#include "build_service/admin/controlflow/JsonParamParser.h"
#include "build_service/admin/controlflow/KeyValueParamParser.h"
#include "build_service/admin/controlflow/ListParamParser.h"
#include "build_service/admin/controlflow/LocalLuaScriptReader.h"
#include "build_service/admin/controlflow/LuaLoggerWrapper.h"
#include "build_service/admin/controlflow/OpenApiHandler.h"
#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/admin/controlflow/TaskContainer.h"
#include "build_service/admin/controlflow/TaskFactory.h"
#include "build_service/admin/controlflow/TaskFlowWrapper.h"
#include "build_service/admin/controlflow/TaskWrapper.h"
#include "build_service/util/ErrorLogCollector.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::util;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, TaskFlow);

#define LOG_PREFIX _logPrefix.c_str()

void TaskFlow::UpstreamItem::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize(TASK_FLOW_UPSTREAM_ID, flowId);
    json.Jsonize(TASK_FLOW_UPSTREAM_MISSING_ACTION, actionForMissing);

    if (json.GetMode() == Jsonizable::TO_JSON) {
        std::string statusStr = waitStatus2Str(waitMask);
        json.Jsonize(TASK_FLOW_UPSTREAM_WAIT_STATUS, statusStr);
    } else {
        std::string statusStr;
        json.Jsonize(TASK_FLOW_UPSTREAM_WAIT_STATUS, statusStr);
        waitMask = str2WaitMask(statusStr);
    }
}

TaskFlow::TaskFlow(const string& rootPath, const TaskFactoryPtr& factory, const TaskContainerPtr& taskContainer,
                   const TaskResourceManagerPtr& taskResMgr)
    : _rootPath(rootPath)
    , _lState(NULL)
    , _taskFactory(factory)
    , _taskContainer(taskContainer)
    , _taskResMgr(taskResMgr)
    , _status(tf_unknown)
    , _lastStatus(tf_unknown)
    , _flowContainer(NULL)
    , _isPublicFlow(false)
{
    if (!_taskContainer) {
        _taskContainer.reset(new TaskContainer);
    } else {
        _logPrefix = _taskContainer->getLogPrefix();
    }
}

TaskFlow::~TaskFlow()
{
    if (_lState) {
        ELuna::closeLua(_lState);
        _lState = NULL;
    }
}

bool TaskFlow::init(const string& fileName, const string& flowId, const KeyValueMap& flowParam)
{
    _flowParamMap = flowParam;
    _flowId = flowId;
    _status = tf_init;

    vector<string> flowInfoVec = {FILE_FLOW_INFO_PREFIX, fileName};
    _scriptInfo = StringUtil::toString(flowInfoVec, "|");

    string fileContent;
    string filePath;
    if (!LocalLuaScriptReader::readScriptFile(_taskResMgr, _rootPath, fileName, filePath, fileContent)) {
        _status = tf_fatal;
        _hasFatalError = true;
        BS_PREFIX_LOG(ERROR, "load flow from file [%s], flowId [%s] fail!", filePath.c_str(), flowId.c_str());
        return false;
    }

    if (!initByString(fileContent)) {
        BS_PREFIX_LOG(ERROR, "load flow from file [%s], flowId [%s] fail!", filePath.c_str(), flowId.c_str());
        return false;
    }

    _filePathIdentifier = LocalLuaScriptReader::getRelativeScriptFileName(filePath, _rootPath);
    if (_filePathIdentifier.empty()) {
        _filePathIdentifier = filePath;
    }
    BS_PREFIX_LOG(INFO, "create flow [%s] from [%s] success!", flowId.c_str(), filePath.c_str());
    return true;
}

bool TaskFlow::initSimpleFlow(const string& taskId, const string& kernalType, const KeyValueMap& kvMap,
                              const string& flowId)
{
    _flowId = flowId;
    _status = tf_init;

    vector<string> flowInfoVec = {SIMPLE_FLOW_INFO_PREFIX, taskId, kernalType};
    _scriptInfo = StringUtil::toString(flowInfoVec, "|");

    _globalParams = kvMap;
    _flowParamMap = kvMap;
    string str = getLuaScriptForSimpleFlow(taskId, kernalType);
    if (!initByString(str)) {
        BS_PREFIX_LOG(ERROR, "load simple flow [%s] for task [id:%s, type:%s] fail!", flowId.c_str(), taskId.c_str(),
                      kernalType.c_str());
        return false;
    }

    BS_PREFIX_LOG(INFO, "create simple flow [%s] for task [id:%s, type:%s] success!", flowId.c_str(), taskId.c_str(),
                  kernalType.c_str());
    return true;
}

bool TaskFlow::isSimpleFlow() const { return _scriptInfo.find(SIMPLE_FLOW_INFO_PREFIX) == 0; }

bool TaskFlow::initByString(const std::string& fileContent)
{
    _lState = ELuna::openLua();
    if (_lState == NULL) {
        _status = tf_fatal;
        _hasFatalError = true;
        return false;
    }
    registerTaskClass(_lState, fileContent);
    if (!initFlowByString(_lState, fileContent)) {
        _status = tf_fatal;
        _hasFatalError = true;
        return false;
    }
    if (!prepareLuaParam(_lState)) {
        _status = tf_fatal;
        _hasFatalError = true;
        return false;
    }
    return true;
}

void TaskFlow::registerTaskClass(lua_State* state, const string& str)
{
    if (enableTool(str)) {
        ListParamWrapper::bindLua(state);
    }

    if (enableLog(str)) {
        LuaLoggerWrapper::bindLua(state);
    }

    KeyValueParamWrapper::bindLua(state);
    JsonParamWrapper::bindLua(state);
    TaskWrapper::bindLua(state);
    TaskFlowWrapper::bindLua(state);
}

bool TaskFlow::initFlowByString(lua_State* state, const string& input)
{
    assert(state);
    string flowStr;
    if (!getFlowString(input, flowStr)) {
        return false;
    }

    BS_PREFIX_LOG(DEBUG, "init control flow by string [%s]", flowStr.c_str());
    return ELuna::doString(state, flowStr.c_str());
}

bool TaskFlow::getFlowString(const string& input, string& flowStr)
{
    if (input.find("function stepRunFlow()") == string::npos) {
        BS_PREFIX_LOG(ERROR, "no stepRunFlow function defined in flow file");
        return false;
    }

    if (input.find("return true") == string::npos) {
        BS_PREFIX_LOG(ERROR, "stepRunFlow should call return true in the end, content [%s]", input.c_str());
        return false;
    }

    string tmp = input;
    tmp = LuaLoggerWrapper::rewrite(input);

    if (enableTool(tmp)) {
        StringUtil::replaceAll(tmp, IMPORT_TOOL_STR, TOOL_SCRIPT_INTERFACE_STR);
    }
    flowStr = FLOW_SCRIPT_INTERFACE_STR + tmp;
    return true;
}

void TaskFlow::removeTask(const string& taskName)
{
    BS_PREFIX_LOG(INFO, "remove task [%s]", taskName.c_str());
    ScopedLock lock(_lock);
    _taskIdSet.erase(taskName);
    _taskContainer->removeTask(getTaskId(_flowId, taskName));
}

bool TaskFlow::stopFlow()
{
    ScopedLock lock(_actionLock);
    BS_PREFIX_LOG(INFO, "stop flow [%s]", _flowId.c_str());
    if (_status == tf_finish) {
        BS_PREFIX_LOG(INFO, " flow [%s] already finish, no need stop", _flowId.c_str());
        return true;
    }

    if (_lState) {
        lua_State* state = _lState;
        _lState = NULL;
        ELuna::closeLua(state);
    }

    vector<TaskBasePtr> tasks = getTasks();
    for (auto& task : tasks) {
        if (task->isTaskStopping() || task->isTaskStopped() || task->isTaskFinish()) {
            BS_PREFIX_LOG(INFO, "task [%s] status is %s, no need stop!", task->getIdentifier().c_str(),
                          TaskBase::taskStatus2Str(task->getTaskStatus()).c_str());
            continue;
        }
        BS_PREFIX_LOG(INFO, "kill task [%s]", task->getIdentifier().c_str());
        if (!task->stop()) {
            BS_PREFIX_LOG(ERROR, "kill task [%s] fail", task->getIdentifier().c_str());
            return false;
        }
    }
    _status = tf_stopping;
    BS_PREFIX_LOG(INFO, "flow [%s] is %s", _flowId.c_str(), status2Str(_status).c_str());
    if (isAllTaskInactive()) {
        _status = tf_stopped;
        BS_PREFIX_LOG(INFO, "flow [%s] is %s", _flowId.c_str(), status2Str(_status).c_str());
    }
    return true;
}

bool TaskFlow::suspendFlow()
{
    ScopedLock lock(_actionLock);
    if (isFlowSuspending()) {
        BS_PREFIX_LOG(INFO, "taskFlow[%s] is suspending, can not be suspend again.", getFlowId().c_str());
        return true;
    }

    BS_PREFIX_LOG(INFO, "suspend flow [%s]", _flowId.c_str());
    vector<TaskBasePtr> tasks = getTasks();
    for (auto& task : tasks) {
        if (task->isTaskSuspended() || task->isTaskSuspending()) {
            BS_PREFIX_LOG(INFO, "task [%s] status is %s, no need to suspend.", task->getIdentifier().c_str(),
                          TaskBase::taskStatus2Str(task->getTaskStatus()).c_str());
            continue;
        }

        if (!task->suspend()) {
            return false;
        }
        BS_PREFIX_LOG(INFO, "suspend task [%s]", task->getIdentifier().c_str());
    }
    _lastStatus = _status;
    _status = tf_suspending;
    BS_PREFIX_LOG(INFO, "flow [%s] is %s", _flowId.c_str(), status2Str(_status).c_str());
    return true;
}

bool TaskFlow::resumeFlow()
{
    ScopedLock lock(_actionLock);
    if (!isFlowSuspended()) {
        BS_PREFIX_LOG(INFO, "taskFlow[%s] is not suspended, no need to resume.", getFlowId().c_str());
        return true;
    }
    BS_PREFIX_LOG(INFO, "resume flow [%s]", _flowId.c_str());

    vector<TaskBasePtr> tasks = getTasks();
    for (auto& task : tasks) {
        assert(task->isTaskSuspended());
        if (!task->resume()) {
            return false;
        }
        BS_PREFIX_LOG(INFO, "resume task [%s]", task->getIdentifier().c_str());
    }
    _status = _lastStatus;
    BS_PREFIX_LOG(INFO, "flow [%s] is %s", _flowId.c_str(), status2Str(_status).c_str());
    return true;
}

bool TaskFlow::prepareLuaParam(lua_State* state)
{
    if (!_taskFactory) {
        BS_PREFIX_LOG(ERROR, "taskFactory is NULL!");
        return false;
    }

    lua_getglobal(state, "_taskFlow_");
    if (!lua_isuserdata(state, -1)) {
        BS_PREFIX_LOG(ERROR, "_taskFlow_ not define in lua script!");
        return false;
    }

    TaskFlowWrapper& ref = ELuna::convert2CppType<TaskFlowWrapper&>::convertType(state, lua_gettop(state));
    ref.setTaskFlow(this);

    ELuna::LuaTable globalParamTable(state, "_globalParam_");
    KeyValueMap::const_iterator iter = _flowParamMap.begin();
    for (; iter != _flowParamMap.end(); iter++) {
        globalParamTable.set(iter->first, iter->second);
    }

    ELuna::LuaTable logMetaTable(state, "_logMeta_");
    logMetaTable.set("prefix", _logPrefix);
    logMetaTable.set("hostId", _flowId);

    vector<string> infoStrs;
    StringUtil::split(infoStrs, _scriptInfo, "|", false);
    if (infoStrs.size() == 2) {
        logMetaTable.set("fileName", infoStrs[1]);
    } else {
        logMetaTable.set("fileName", string("SIMPLE_FLOW|") + infoStrs[1]);
    }
    return true;
}

void TaskFlow::stepRun()
{
    ScopedLock lock(_actionLock);
    if (_status == tf_stopping) {
        if (isAllTaskInactive()) {
            _status = tf_stopped;
            BS_PREFIX_LOG(INFO, "flow [%s] is %s", _flowId.c_str(), status2Str(_status).c_str());
        }
        return;
    }

    if (_status == tf_suspending) {
        if (isAllTaskInSuspended()) {
            _status = tf_suspended;
            BS_PREFIX_LOG(INFO, "flow [%s] is %s", _flowId.c_str(), status2Str(_status).c_str());
        }
        return;
    }

    if (_status == tf_fatal) {
        BS_PREFIX_LOG(ERROR, "will stop current flow [id:%s] for fatal error, msg [%s]", _flowId.c_str(),
                      _errorMsg.c_str());
        stopFlow();
        return;
    }

    if (_status == tf_finishing) {
        if (isAllTaskInactive()) {
            _status = tf_finish;
            BS_PREFIX_LOG(INFO, "flow [%s] is %s", _flowId.c_str(), status2Str(_status).c_str());
        }
        return;
    }

    if (_status == tf_error) {
        BS_INTERVAL_LOG2(20, ERROR, "flow [id:%s] has runtime error, error msg [%s]", _flowId.c_str(),
                         _errorMsg.c_str());
    }

    if (_status != tf_running && _status != tf_error) {
        return;
    }

    bool needAutoStop = false;
    if (!checkUpstreamFlow(needAutoStop)) {
        if (needAutoStop) {
            stopFlow();
        }
        return;
    }

    if (!_lState) {
        _status = tf_fatal;
        _hasFatalError = true;
        return;
    }
    _stepErrorInfo.reset();
    ELuna::LuaFunction<bool> stepRun(_lState, "stepRunFlow");
    bool ret = stepRun();
    if (stepRun.hasError()) {
        BS_PREFIX_LOG(ERROR, "flowId [id:%s] run lua script [%s] fail, check stdout!", _flowId.c_str(),
                      _scriptInfo.c_str());
        string tmp = string("flowId [") + _flowId +
                     "] run lua stepRunFlow function fail,"
                     " script info:" +
                     _scriptInfo;
        setErrorMsg(tmp);
        _status = tf_fatal;
        _hasFatalError = true;
        return;
    }

    if (ret) { // return true means stepRun to eof
        _status = isAllTaskInactive() ? tf_finish : tf_finishing;
        BS_PREFIX_LOG(INFO, "flow [%s] is %s", _flowId.c_str(), status2Str(_status).c_str());
        return;
    }

    // update status & error msg
    if (_stepErrorInfo.errorType == 0) {
        _status = tf_running;
    } else if (_stepErrorInfo.errorType == 1) {
        _status = tf_error;
    } else if (_stepErrorInfo.errorType == 2) {
        _status = tf_fatal;
        _hasFatalError = true;
    }
    setErrorMsg(_stepErrorInfo.errorMsg);
}

TaskBasePtr TaskFlow::createTask(const string& id, const string& kernalType, const KeyValueMap& kvMap)
{
    if (id.find(".") != string::npos) {
        BS_PREFIX_LOG(ERROR, "invalid taskId [%s], should not has \".\" in taskId", id.c_str());
        return TaskBasePtr();
    }

    TaskBasePtr task = getTask(id);
    if (task) {
        if (task->getTaskType() != kernalType) {
            BS_PREFIX_LOG(ERROR, "createTask for id [%s] fail, exist with different kernalType [%s:%s]",
                          getTaskId(_flowId, id).c_str(), kernalType.c_str(), task->getTaskType().c_str());
            return TaskBasePtr();
        }
        addTaskId(id);
        return task;
    }

    if (!_taskFactory) {
        return TaskBasePtr();
    }
    KeyValueMap taskKVMap = getKVMapForTask(id, kvMap);
    string taskId = getTaskId(_flowId, id);
    task = _taskFactory->createTask(taskId, kernalType, taskKVMap, _taskResMgr);
    if (task && _taskContainer->addTask(task)) {
        BS_PREFIX_LOG(INFO, "create task [%s], type [%s] with params [%s] success!", taskId.c_str(), kernalType.c_str(),
                      ToJsonString(taskKVMap).c_str());
        addTaskId(id);
        return task;
    }
    BS_PREFIX_LOG(INFO, "create task [%s], type [%s] with params [%s] failed!", taskId.c_str(), kernalType.c_str(),
                  ToJsonString(taskKVMap).c_str());
    return TaskBasePtr();
}

string TaskFlow::getTaskId(const string& flowId, const string& id) { return flowId.empty() ? id : flowId + "." + id; }

void TaskFlow::getOriginalFlowIdAndTaskId(const std::string& taskId, std::string& flowId, std::string& id)
{
    auto location = taskId.find(".");
    if (location != std::string::npos) {
        flowId = taskId.substr(0, location);
        id = taskId.substr(location + 1);
    } else {
        id = taskId;
    }
}

TaskBasePtr TaskFlow::getTask(const string& id) const
{
    string taskId = getTaskId(_flowId, id);
    return _taskContainer->getTask(taskId);
}

TaskBasePtr TaskFlow::getTaskInFlow(const string& id, const string& flowId) const
{
    if (flowId == _flowId) {
        return getTask(id);
    }

    TaskFlowPtr flow = getFlow(flowId);
    if (!flow) {
        return TaskBasePtr();
    }
    return flow->getTask(id);
}

TaskFlowPtr TaskFlow::getFlow(const string& flowId) const
{
    if (!_flowContainer) {
        return TaskFlowPtr();
    }

    TaskFlowPtr flow = _flowContainer->getFlow(flowId);
    if (flow && !flow->isFriendFlow(_flowId)) {
        BS_PREFIX_LOG(WARN,
                      "check flow friendship fail, "
                      "flow [%s] is not a friend for flow [%s]",
                      _flowId.c_str(), flowId.c_str());
        return TaskFlowPtr();
    }
    return flow;
}

void TaskFlow::addUpstreamFlowId(const string& flowId, const string& waitStatus, const string& actionForMissing)
{
    if (flowId.empty()) {
        BS_PREFIX_LOG(ERROR, "empty flowId");
        return;
    }

    uint32_t waitStatMask = str2WaitMask(waitStatus);
    UpstreamItem item;
    item.flowId = flowId;
    item.waitMask = waitStatMask;
    item.actionForMissing = 0;
    if (actionForMissing == "ignore") {
        item.actionForMissing = 1;
    }

    ScopedLock lock(_lock);
    for (size_t i = 0; i < _upstreamFlowInfos.size(); i++) {
        if (_upstreamFlowInfos[i].flowId == flowId) {
            _upstreamFlowInfos[i] = item;
            return;
        }
    }
    _upstreamFlowInfos.push_back(item);
}

void TaskFlow::addFriendFlowId(const string& flowId)
{
    if (flowId.empty()) {
        BS_PREFIX_LOG(ERROR, "empty flowId");
        return;
    }

    ScopedLock lock(_lock);
    for (size_t i = 0; i < _friendFlows.size(); i++) {
        if (_friendFlows[i] == flowId) {
            return;
        }
    }
    _friendFlows.push_back(flowId);
}

bool TaskFlow::isUpstreamFlowReady()
{
    bool needAutoStop = false;
    return checkUpstreamFlow(needAutoStop);
}

bool TaskFlow::checkUpstreamFlow(bool& needAutoStop)
{
    needAutoStop = false;
    std::vector<UpstreamItem> upstreamFlowInfos;
    {
        ScopedLock lock(_lock);
        upstreamFlowInfos = _upstreamFlowInfos;
    }

    if (upstreamFlowInfos.empty()) {
        return true;
    }

    if (!_flowContainer) {
        BS_PREFIX_LOG(WARN, "flow container is null");
        return true;
    }

    vector<string> matchFlows;
    for (auto& info : upstreamFlowInfos) {
        TaskFlowPtr flow = _flowContainer->getFlow(info.flowId);
        if (!flow) {
            if (info.actionForMissing == 2) {
                // already pass before
                continue;
            }
            if (info.actionForMissing == 0) {
                BS_PREFIX_LOG(WARN,
                              "upstream task flow [%s] not exist, "
                              "current flow[%s] will auto stop!",
                              info.flowId.c_str(), _flowId.c_str());
                needAutoStop = true;
                return false;
            }
            assert(info.actionForMissing == 1);
            BS_PREFIX_LOG(WARN,
                          "upstream task flow [%s] not exist, "
                          "current flow[%s] will ignore upstream flow!",
                          info.flowId.c_str(), _flowId.c_str());
            continue;
        }
        uint32_t matchValue = (flow->_status & info.waitMask);
        if (matchValue == 0) {
            BS_INTERVAL_LOG2(120, INFO,
                             "flow [id:%s,wait:%s] check upstream fail,"
                             " flow [id:%s,status:%s] not ready",
                             _flowId.c_str(), waitStatus2Str(info.waitMask).c_str(), flow->getFlowId().c_str(),
                             status2Str(flow->_status).c_str());
            return false;
        }
        BS_PREFIX_LOG(DEBUG,
                      "flow [id:%s,wait:%s] check upstream pass,"
                      " upstream flow [id:%s,status:%s]",
                      _flowId.c_str(), waitStatus2Str(info.waitMask).c_str(), flow->getFlowId().c_str(),
                      status2Str(flow->_status).c_str());
        matchFlows.push_back(info.flowId);
    }

    if (!matchFlows.empty()) {
        ScopedLock lock(_lock);
        for (auto& info : _upstreamFlowInfos) {
            if (info.actionForMissing == 2) {
                continue;
            }
            if (find(matchFlows.begin(), matchFlows.end(), info.flowId) != matchFlows.end()) {
                info.actionForMissing = 2;
            }
        }
    }
    return true;
}

bool TaskFlow::isFriendFlow(const string& flowId) const
{
    if (_isPublicFlow) {
        return true;
    }
    vector<string> friendFlows;
    {
        ScopedLock lock(_lock);
        friendFlows = _friendFlows;
    }

    for (auto& item : friendFlows) {
        if (item == flowId) {
            return true;
        }
    }
    return false;
}

void TaskFlow::setTaskParameter(const TaskParameterMap& taskMap)
{
    ScopedLock lock(_lock);
    _taskKVMap = taskMap;
}

void TaskFlow::setGlobalTaskParam(const string& key, const string& value)
{
    ScopedLock lock(_lock);
    _globalParams[key] = value;
}

KeyValueMap TaskFlow::getKVMapForTask(const string& taskId, const KeyValueMap& kvParams)
{
    ScopedLock lock(_lock);
    KeyValueMap ret = _globalParams;
    auto fillKVMap = [&ret](const KeyValueMap& map) {
        KeyValueMap::const_iterator iter = map.begin();
        for (; iter != map.end(); iter++) {
            ret[iter->first] = iter->second;
        }
    };

    TaskParameterMap::const_iterator iter = _taskKVMap.find(taskId);
    if (iter != _taskKVMap.end()) {
        fillKVMap(iter->second);
    }
    fillKVMap(kvParams);
    return ret;
}

string TaskFlow::status2Str(TF_STATUS status)
{
    if (status == tf_init) {
        return "init";
    }
    if (status == tf_running) {
        return "running";
    }
    if (status == tf_finishing) {
        return "finishing";
    }
    if (status == tf_finish) {
        return "finish";
    }
    if (status == tf_stopping) {
        return "stopping";
    }
    if (status == tf_stopped) {
        return "stopped";
    }
    if (status == tf_suspending) {
        return "suspending";
    }
    if (status == tf_suspended) {
        return "suspended";
    }
    if (status == tf_error) {
        return "error";
    }
    if (status == tf_fatal) {
        return "fatal";
    }
    return "unknown";
}

TaskFlow::TF_STATUS TaskFlow::str2Status(const string& status)
{
    if (status == "init") {
        return tf_init;
    }
    if (status == "running") {
        return tf_running;
    }

    if (status == "eof" || status == "finishing") {
        return tf_finishing;
    }
    if (status == "finish") {
        return tf_finish;
    }
    if (status == "stopped") {
        return tf_stopped;
    }
    if (status == "stopping") {
        return tf_stopping;
    }
    if (status == "suspended") {
        return tf_suspended;
    }
    if (status == "suspending") {
        return tf_suspending;
    }
    if (status == "error") {
        return tf_error;
    }
    if (status == "fatal") {
        return tf_fatal;
    }
    return tf_unknown;
}

string TaskFlow::waitStatus2Str(uint32_t waitMask)
{
    vector<string> strs;
#define ADD_WAIT_STATUS_STR(ST)                                                                                        \
    if (waitMask & ST) {                                                                                               \
        strs.push_back(status2Str(ST));                                                                                \
    }

    ADD_WAIT_STATUS_STR(tf_init);
    ADD_WAIT_STATUS_STR(tf_running);
    ADD_WAIT_STATUS_STR(tf_finishing);
    ADD_WAIT_STATUS_STR(tf_finish);
    ADD_WAIT_STATUS_STR(tf_stopping);
    ADD_WAIT_STATUS_STR(tf_stopped);
    ADD_WAIT_STATUS_STR(tf_suspending);
    ADD_WAIT_STATUS_STR(tf_suspended);
    ADD_WAIT_STATUS_STR(tf_error);
    ADD_WAIT_STATUS_STR(tf_fatal);

    return StringUtil::toString(strs, "|");
}

uint32_t TaskFlow::str2WaitMask(const string& waitStatus)
{
    vector<string> statusVec;
    StringUtil::fromString(waitStatus, statusVec, "|");
    uint32_t waitStatMask = 0;
    for (size_t i = 0; i < statusVec.size(); i++) {
        waitStatMask |= (uint32_t)str2Status(statusVec[i]);
    }

    if (waitStatMask == 0) {
        // set default wait status
        waitStatMask = tf_finish;
    }
    return waitStatMask;
}

void TaskFlow::makeActive(bool force)
{
    if (!force && _status != tf_init) {
        return;
    }

    BS_PREFIX_LOG(INFO, "make flow [%s] active!", _flowId.c_str());
    _status = tf_running;
    _hasFatalError = false;
}

void TaskFlow::addTaskId(const string& taskId)
{
    ScopedLock lock(_lock);
    _taskIdSet.insert(taskId);
}

void TaskFlow::clearTasks()
{
    ScopedLock lock(_lock);
    set<string>::const_iterator iter = _taskIdSet.begin();
    for (; iter != _taskIdSet.end(); iter++) {
        _taskContainer->removeTask(getTaskId(_flowId, *iter));
    }
    _taskIdSet.clear();
}

vector<TaskBasePtr> TaskFlow::getTasks() const
{
    ScopedLock lock(_lock);
    vector<TaskBasePtr> ret;
    ret.reserve(_taskIdSet.size());

    set<string>::const_iterator iter = _taskIdSet.begin();
    for (; iter != _taskIdSet.end(); iter++) {
        TaskBasePtr task = getTask(*iter);
        if (!task) {
            BS_PREFIX_LOG(ERROR, "get null task object taskId[%s] in flow[%s]", (*iter).c_str(), _flowId.c_str());
            continue;
        }
        ret.push_back(task);
    }
    return ret;
}

bool TaskFlow::isAllTaskInactive() const
{
    vector<TaskBasePtr> tasks = getTasks();
    for (auto& task : tasks) {
        if (task->isTaskRunning() || task->isTaskStopping() || task->isTaskFinishing()) {
            return false;
        }
    }
    BS_PREFIX_LOG(INFO,
                  "all tasks in flow [%s] are in-active "
                  "(not running or stopping)!",
                  _flowId.c_str());
    return true;
}

bool TaskFlow::isAllTaskInSuspended() const
{
    vector<TaskBasePtr> tasks = getTasks();
    for (auto& task : tasks) {
        if (!task->isTaskSuspended()) {
            return false;
        }
    }
    return true;
}

void TaskFlow::Jsonize(Jsonizable::JsonWrapper& json)
{
    ScopedLock lock(_lock);
    json.Jsonize(FLOW_ID_STRING, _flowId);
    json.Jsonize(FLOW_IS_PUBLIC_STRING, _isPublicFlow);
    json.Jsonize(FLOW_HAS_FATAL_ERROR, _hasFatalError);
    json.Jsonize(FLOW_SCRIPT_INFO_STR, _scriptInfo);
    json.Jsonize(FLOW_TASK_PARAM_STR, _taskKVMap);
    json.Jsonize(FLOW_GLOBAL_PARAM_STR, _globalParams);
    json.Jsonize(FLOW_UPSTREAM_INFO_STR, _upstreamFlowInfos);
    json.Jsonize(FLOW_FRIEND_STR, _friendFlows);
    json.Jsonize(FLOW_ERROR_MSG_STR, _errorMsg, _errorMsg);
    json.Jsonize(FLOW_PROPERTY_STR, _propertyMap);
    json.Jsonize(FLOW_PARAM_STR, _flowParamMap);
    json.Jsonize(FLOW_TAG_STR, _tags);
    json.Jsonize(FLOW_GRAPH_ID_STR, _graphId, _graphId);
    if (json.GetMode() == TO_JSON) {
        string statusStr = status2Str(_status);
        string lastStatusStr = status2Str(_lastStatus);
        json.Jsonize(FLOW_STATUS_STR, statusStr);
        json.Jsonize(FLOW_LAST_STATUS_STR, lastStatusStr);
        vector<string> taskIds(_taskIdSet.begin(), _taskIdSet.end());
        json.Jsonize(FLOW_TASKID_SET_STR, taskIds);
    } else {
        string statusStr;
        string lastStatusStr;
        json.Jsonize(FLOW_STATUS_STR, statusStr);
        json.Jsonize(FLOW_LAST_STATUS_STR, lastStatusStr);
        _status = str2Status(statusStr);
        _lastStatus = str2Status(lastStatusStr);

        vector<string> taskIds;
        json.Jsonize(FLOW_TASKID_SET_STR, taskIds);
        _taskIdSet.insert(taskIds.begin(), taskIds.end());

        string content;
        if (!getLuaScriptFromScriptInfo(_scriptInfo, content)) {
            throw autil::legacy::ExceptionBase("invalid script info [" + _scriptInfo + "]");
        }
        if (!initByString(content)) {
            throw autil::legacy::ExceptionBase("invalid script content [" + content + "]");
        }
    }
}

string TaskFlow::getLuaScriptForSimpleFlow(const string& taskId, const string& taskType)
{
    string str = SIMPLE_FLOW_TEMPLATE;
    StringUtil::replaceAll(str, "%TASK_NAME%", taskId);
    StringUtil::replaceAll(str, "%TASK_TYPE%", taskType);
    return str;
}

bool TaskFlow::getLuaScriptFromScriptInfo(const string& scriptInfo, string& content)
{
    vector<string> infoStrs;
    StringUtil::split(infoStrs, scriptInfo, "|", false);
    if (infoStrs.size() != 2 && infoStrs.size() != 3) {
        BS_PREFIX_LOG(ERROR, "invalid script info [%s]", scriptInfo.c_str());
        return false;
    }

    if (infoStrs[0] == FILE_FLOW_INFO_PREFIX && infoStrs.size() == 2) {
        string filePath;
        if (!LocalLuaScriptReader::readScriptFile(_taskResMgr, _rootPath, infoStrs[1], filePath, content)) {
            BS_PREFIX_LOG(ERROR, "read script from file [%s] fail", filePath.c_str());
            return false;
        }
        _filePathIdentifier = LocalLuaScriptReader::getRelativeScriptFileName(filePath, _rootPath);
        if (_filePathIdentifier.empty()) {
            _filePathIdentifier = filePath;
        }
        return true;
    }

    if (infoStrs[0] == SIMPLE_FLOW_INFO_PREFIX && infoStrs.size() == 3) {
        content = getLuaScriptForSimpleFlow(infoStrs[1], infoStrs[2]);
        return true;
    }
    BS_PREFIX_LOG(ERROR, "invalid script info [%s]", scriptInfo.c_str());
    return false;
}

void TaskFlow::setError(const string& errorMsg, bool isFatal)
{
    _stepErrorInfo.errorType = (!isFatal ? 1 : 2);
    _stepErrorInfo.errorMsg = errorMsg;
}

string TaskFlow::getError() const
{
    ScopedLock lock(_lock);
    return _errorMsg;
}

void TaskFlow::setErrorMsg(const string& str)
{
    ScopedLock lock(_lock);
    _errorMsg = str;
}

const string& TaskFlow::getProperty(const string& key) const
{
    KeyValueMap::const_iterator iter = _propertyMap.find(key);
    if (iter == _propertyMap.end()) {
        static std::string emptyStr;
        return emptyStr;
    }
    return iter->second;
}

const std::string& TaskFlow::getFlowProperty(const std::string& flowId, const std::string& key)
{
    TaskFlowPtr flow = _flowContainer->getFlow(flowId);
    if (!flow) {
        static string emptyString;
        return emptyString;
    }
    return flow->getProperty(key);
}

const std::string& TaskFlow::getFlowStatus(const std::string& flowId)
{
    TaskFlowPtr flow = _flowContainer->getFlow(flowId);
    if (!flow) {
        static string emptyString = TaskFlow::status2Str(tf_unknown);
        return emptyString;
    }
    return flow->getStatusStr();
}

void TaskFlow::setProperty(const string& key, const string& value) { _propertyMap[key] = value; }

vector<string> TaskFlow::getUpstreamItemInfos() const
{
    vector<string> ret;
    ret.reserve(_upstreamFlowInfos.size());
    for (auto& upstreamInfo : _upstreamFlowInfos) {
        string info = upstreamInfo.flowId + ":" + waitStatus2Str(upstreamInfo.waitMask) + ":" +
                      StringUtil::toString(upstreamInfo.actionForMissing);
        ret.push_back(info);
    }
    return ret;
}

vector<string> TaskFlow::getUpstreamFlowIds() const
{
    vector<string> ret;
    ret.reserve(_upstreamFlowInfos.size());
    for (auto& upstreamInfo : _upstreamFlowInfos) {
        ret.push_back(upstreamInfo.flowId);
    }
    return ret;
}

vector<string> TaskFlow::getFriendFlowIds() const { return _friendFlows; }

string TaskFlow::getTags() const { return StringUtil::toString(_tags.begin(), _tags.end()); }

string TaskFlow::getFlowLableString(bool fillTask) const
{
    std::vector<UpstreamItem> upstreamFlowInfos;
    {
        ScopedLock lock(_lock);
        upstreamFlowInfos = _upstreamFlowInfos;
    }

    string edgeInfo;
    bool hasBlockUpstream = false;
    for (auto& info : upstreamFlowInfos) {
        edgeInfo += info.flowId + "->" + _flowId + "[style=bold,label=";
        string lableName = "wait:" + waitStatus2Str(info.waitMask);
        string color;
        lableName += "[";
        if (info.actionForMissing == 2) {
            lableName += "pass]";
            color = "green";
        } else if (!_flowContainer) {
            lableName += "unknown]";
            color = "black";
        } else if (_flowContainer->getFlow(info.flowId)) {
            lableName += "block]";
            color = "yellow";
            hasBlockUpstream = true;
        } else if (info.actionForMissing == 0) {
            lableName += "stop]";
            color = "red";
        } else {
            assert(info.actionForMissing == 1);
            lableName += "ignore]";
            color = "blue";
        }
        edgeInfo += "\"";
        edgeInfo += EscapeForCCode(lableName);
        edgeInfo += "\"";
        edgeInfo += ",color=" + color + "]\n";
    }

    string ret = getFlowInfoLableStr(hasBlockUpstream);
    string taskInfoStr;
    if (fillTask) {
        taskInfoStr = getInFlowTaskNodeStr();
    }
    if (!taskInfoStr.empty()) {
        ret += taskInfoStr;
    }
    ret += edgeInfo;
    return ret;
}

string TaskFlow::getFlowInfoLableStr(bool hasBlockUpstream) const
{
    ScopedLock lock(_actionLock);
    string ret = _flowId + "[shape=box,label=";

    vector<string> infoStrs;
    StringUtil::split(infoStrs, _scriptInfo, "|", false);
    KeyValueMap info;
    if (isSimpleFlow()) {
        assert(infoStrs.size() == 3);
        info["simpleFlowTask"] = infoStrs[1] + "|" + infoStrs[2];
    } else {
        assert(infoStrs.size() == 2);
        info["fileName"] = infoStrs[1];
        info["filePath"] = _filePathIdentifier;
    }

    if (!_tags.empty()) {
        info["tags"] = getTags();
    }
    if (!_errorMsg.empty()) {
        info["errorMsg"] = _errorMsg;
    }
    if (!_graphId.empty()) {
        info["graphId"] = _graphId;
    }
    info["flowStatus"] = status2Str(_status);
    info["lastFlowStatus"] = status2Str(_lastStatus);
    for (auto item : _propertyMap) {
        info[item.first + "_@_property"] = item.second;
    }
    ret += "\"";
    ret += EscapeForCCode(string("flowID:") + _flowId + "\n" + ToJsonString(info));
    ret += "\"";
    if (_hasFatalError) {
        ret += ",color=red";
    } else if (isFlowRunning()) {
        ret += hasBlockUpstream ? ",color=yellow" : ",color=green";
    } else if (isFlowFinishing()) {
        ret += ",color=blue";
    }
    ret += "];\n";
    return ret;
}

string TaskFlow::getInFlowTaskNodeStr() const
{
    vector<TaskBasePtr> tasks = getTasks();
    if (tasks.empty()) {
        return string("");
    }

    string taskId = "\"" + tasks[0]->getIdentifier() + "\"";
    string lableName = string("cluster_tasksInFlow") + _flowId;
    string ret = "subgraph " + lableName + " {\n";
    ret += "style=filled;\n";
    ret += "color=lightgrey;\n";
    ret += "label=\"tasksInFlow" + _flowId + "\";\n";
    for (auto& task : tasks) {
        ret += task->getTaskLabelString(nullptr);
    }
    ret += "}\n";
    ret += taskId + "->" + _flowId + "[arrowhead=none,label=\"inner_tasks\",";
    ret += "style=dotted,ltail=" + lableName + "];\n";
    return ret;
}

void TaskFlow::fillInnerTaskInfo(std::string& taskDotString, std::map<std::string, KeyValueMap>& taskDetailInfoMap)
{
    vector<TaskBasePtr> tasks = getTasks();
    if (tasks.empty()) {
        return;
    }
    string lableName = string("cluster_tasksInFlow") + _flowId;
    string ret = "digraph " + lableName + " {\n";
    ret += "style=filled;\n";
    ret += "color=lightgrey;\n";
    ret += "label=\"tasksInFlow" + _flowId + "\";\n";
    for (auto& task : tasks) {
        KeyValueMap& detailInfo = taskDetailInfoMap[task->getIdentifier()];
        ret += task->getTaskLabelString(&detailInfo);
    }
    ret += "}\n";
    taskDotString = ret;
}

bool TaskFlow::openApi(const std::string& cmd, const std::string& path, const KeyValueMap& params)
{
    OpenApiHandler handler(_taskResMgr);
    return handler.handle(cmd, path, params);
}

#undef LOG_PREFIX
}} // namespace build_service::admin
