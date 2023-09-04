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
#include "build_service/admin/controlflow/TaskWrapper.h"

#include "build_service/admin/controlflow/ControlDefine.h"
#include "build_service/admin/controlflow/KeyValueParamParser.h"
#include "build_service/admin/controlflow/TaskBase.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, TaskWrapper);

TaskWrapper::TaskWrapper() {}

TaskWrapper::~TaskWrapper() {}

const char* TaskWrapper::getIdentifier()
{
    if (!_taskPtr) {
        return NULL;
    }
    return _taskPtr->getIdentifier().c_str();
}

const char* TaskWrapper::getType()
{
    if (!_taskPtr) {
        return NULL;
    }
    return _taskPtr->getTaskType().c_str();
}

int64_t TaskWrapper::getEndTime()
{
    if (!_taskPtr) {
        return 0;
    }
    return _taskPtr->getEndTime();
}

const char* TaskWrapper::getProperty(const char* key)
{
    if (!_taskPtr) {
        return "";
    }
    return _taskPtr->getProperty(key).c_str();
}

bool TaskWrapper::setProperty(const char* key, const char* value)
{
    if (!_taskPtr) {
        return false;
    }
    return _taskPtr->setPropertyByAuthority(key, value, true);
}

bool TaskWrapper::start(const char* kvParamStr)
{
    if (!_taskPtr) {
        return false;
    }
    if (_taskPtr->isTaskStopping()) {
        BS_LOG(ERROR, "task [%s] still stopping!", _taskPtr->getIdentifier().c_str());
        return false;
    }
    if (_taskPtr->isTaskFinishing()) {
        BS_LOG(DEBUG, "task [%s] still finishing!", _taskPtr->getIdentifier().c_str());
        return false;
    }
    if (_taskPtr->isTaskRunning()) {
        // already start
        BS_LOG(DEBUG, "task [%s] already start!", _taskPtr->getIdentifier().c_str());
        return true;
    }

    KeyValueMap kvMap;
    if (!KeyValueParamParser::parseFromJsonString(kvParamStr, kvMap)) {
        BS_LOG(ERROR, "parse kv param fail!");
        return false;
    }
    if (!_taskPtr->start(kvMap)) {
        BS_LOG(ERROR, "start task [%s] fail!", _taskPtr->getIdentifier().c_str());
        return false;
    }

    if (_taskPtr->getTaskStatus() == TaskBase::ts_init || _taskPtr->getTaskStatus() == TaskBase::ts_finish ||
        _taskPtr->getTaskStatus() == TaskBase::ts_stopped) {
        BS_LOG(WARN, "task [%s] status still be [%s] after start success, rewrite to running",
               _taskPtr->getIdentifier().c_str(), TaskBase::taskStatus2Str(_taskPtr->getTaskStatus()).c_str());
        _taskPtr->setTaskStatus(TaskBase::ts_running);
    }
    return true;
}

bool TaskWrapper::executeCmd(const char* cmdName, const char* kvParamStr)
{
    if (!_taskPtr) {
        return false;
    }
    KeyValueMap kvMap;
    if (!KeyValueParamParser::parseFromJsonString(kvParamStr, kvMap)) {
        BS_LOG(ERROR, "parse kv param fail!");
        return false;
    }
    string cmd(cmdName);
    return _taskPtr->executeCmd(cmd, kvMap);
}

bool TaskWrapper::finish(const char* kvParamStr)
{
    if (!_taskPtr) {
        return false;
    }

    if (_taskPtr->isTaskStopping() || _taskPtr->isTaskStopped() || _taskPtr->isTaskFinishing() ||
        _taskPtr->isTaskFinish()) {
        BS_LOG(DEBUG, "task [%s] is %s, no need finish!", _taskPtr->getIdentifier().c_str(),
               TaskBase::taskStatus2Str(_taskPtr->getTaskStatus()).c_str());
        return true;
    }

    KeyValueMap kvMap;
    if (!KeyValueParamParser::parseFromJsonString(kvParamStr, kvMap)) {
        BS_LOG(ERROR, "parse kv param fail!");
        return false;
    }
    if (!_taskPtr->finish(kvMap)) {
        BS_LOG(ERROR, "finish task [%s] fail!", _taskPtr->getIdentifier().c_str());
        return false;
    }
    return true;
}

bool TaskWrapper::isValid() { return _taskPtr.get() != NULL; }

bool TaskWrapper::isTaskFinish()
{
    if (!_taskPtr) {
        return false;
    }
    return _taskPtr->isTaskFinish();
}

bool TaskWrapper::isTaskFinishing()
{
    if (!_taskPtr) {
        return false;
    }
    return _taskPtr->isTaskFinishing();
}

bool TaskWrapper::isTaskStopped()
{
    if (!_taskPtr) {
        return false;
    }
    return _taskPtr->isTaskStopped();
}

bool TaskWrapper::isTaskStopping()
{
    if (!_taskPtr) {
        return false;
    }
    return _taskPtr->isTaskStopping();
}

bool TaskWrapper::isTaskRunning()
{
    if (!_taskPtr) {
        return false;
    }
    return _taskPtr->isTaskRunning();
}

bool TaskWrapper::isTaskError()
{
    if (!_taskPtr) {
        return false;
    }
    return _taskPtr->isTaskError();
}

void TaskWrapper::bindLua(lua_State* state)
{
    assert(state);
    ELuna::registerClass<TaskWrapper>(state, "TaskWrapper", ELuna::constructor<TaskWrapper>);
    ELuna::registerMethod<TaskWrapper>(state, "getIdentifier", &TaskWrapper::getIdentifier);
    ELuna::registerMethod<TaskWrapper>(state, "getType", &TaskWrapper::getType);
    ELuna::registerMethod<TaskWrapper>(state, "getProperty", &TaskWrapper::getProperty);
    ELuna::registerMethod<TaskWrapper>(state, "getEndTime", &TaskWrapper::getEndTime);
    ELuna::registerMethod<TaskWrapper>(state, "setProperty", &TaskWrapper::setProperty);
    ELuna::registerMethod<TaskWrapper>(state, "isValid", &TaskWrapper::isValid);
    ELuna::registerMethod<TaskWrapper>(state, "isTaskFinish", &TaskWrapper::isTaskFinish);
    ELuna::registerMethod<TaskWrapper>(state, "isTaskFinishing", &TaskWrapper::isTaskFinishing);
    ELuna::registerMethod<TaskWrapper>(state, "isTaskStopping", &TaskWrapper::isTaskStopping);
    ELuna::registerMethod<TaskWrapper>(state, "isTaskStopped", &TaskWrapper::isTaskStopped);
    ELuna::registerMethod<TaskWrapper>(state, "isTaskRunning", &TaskWrapper::isTaskRunning);
    ELuna::registerMethod<TaskWrapper>(state, "isTaskError", &TaskWrapper::isTaskError);
    ELuna::registerMethod<TaskWrapper>(state, "start", &TaskWrapper::start);
    ELuna::registerMethod<TaskWrapper>(state, "finish", &TaskWrapper::finish);
    ELuna::registerMethod<TaskWrapper>(state, "executeCmd", &TaskWrapper::executeCmd);
}

}} // namespace build_service::admin
