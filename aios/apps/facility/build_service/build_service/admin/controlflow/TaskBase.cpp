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
#include "build_service/admin/controlflow/TaskBase.h"

#include "build_service/admin/TaskStatusMetricReporter.h"
#include "build_service/admin/controlflow/ControlDefine.h"
#include "build_service/admin/controlflow/KeyValueParamParser.h"

using namespace std;
using namespace build_service::proto;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace build_service { namespace admin {

BS_LOG_SETUP(admin, TaskBase);

const std::string TaskBase::TASK_STATUS_PROPERTY_KEY = "_task_status_";
const std::string TaskBase::LAST_TASK_STATUS_PROPERTY_KEY = "_last_task_status_";

void TaskBase::TaskMeta::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize(TASK_META_ID, _taskId);
    json.Jsonize(TASK_META_TYPE, _taskType);
    json.Jsonize(TASK_META_PROPERTY, _propertyMap);
    json.Jsonize("begin_time", _beginTime, _beginTime);
    json.Jsonize("end_time", _endTime, _endTime);
}

TaskBase::TaskBase(const string& id, const string& type, const TaskResourceManagerPtr& resMgr)
    : _taskId(id)
    , _taskType(type)
    , _resourceManager(resMgr)
    , _beginTime(0)
    , _endTime(0)
{
    _kmonTags.AddTag("taskId", _taskId);
    _kmonTags.AddTag("taskType", _taskType);
}

TaskBase::~TaskBase() {}

bool TaskBase::getValueFromKVMap(const KeyValueMap& kvMap, const string& key, string& value)
{
    auto iter = kvMap.find(key);
    if (iter == kvMap.end()) {
        return false;
    }
    value = iter->second;
    return true;
}

const string& TaskBase::getProperty(const string& key) const
{
    KeyValueMap::const_iterator iter = _propertyMap.find(key);
    if (iter == _propertyMap.end()) {
        static std::string emptyStr;
        return emptyStr;
    }
    return iter->second;
}

bool TaskBase::setProperty(const string& key, const string& value) { return setPropertyByAuthority(key, value, false); }

bool TaskBase::setPropertyByAuthority(const string& key, const string& value, bool luaUser)
{
    if (key == TASK_STATUS_PROPERTY_KEY) {
        return false;
    }
    if (luaUser && !CheckKeyAuthority(key)) {
        return false;
    }
    _propertyMap[key] = value;
    return true;
}

bool TaskBase::CheckKeyAuthority(const string& key) { return false; }

string TaskBase::taskStatus2Str(TaskStatus stat)
{
    switch (stat) {
    case TaskBase::ts_init:
        return string("init");
    case TaskBase::ts_running:
        return string("running");
    case TaskBase::ts_finishing:
        return string("finishing");
    case TaskBase::ts_finish:
        return string("finish");
    case TaskBase::ts_stopping:
        return string("stopping");
    case TaskBase::ts_stopped:
        return string("stopped");
    case TaskBase::ts_error:
        return string("error");
    case TaskBase::ts_suspended:
        return string("suspended");
    case TaskBase::ts_suspending:
        return string("suspending");
    case TaskBase::ts_unknown:
        return string("unknown");
    default:
        assert(false);
        return string("");
    }

    assert(false);
    return string("");
}

TaskBase::TaskStatus TaskBase::str2TaskStatus(const string& str)
{
    if (str == "init") {
        return TaskBase::ts_init;
    }
    if (str == "running") {
        return TaskBase::ts_running;
    }
    if (str == "finish") {
        return TaskBase::ts_finish;
    }
    if (str == "finishing") {
        return TaskBase::ts_finishing;
    }
    if (str == "stopping") {
        return TaskBase::ts_stopping;
    }
    if (str == "stopped") {
        return TaskBase::ts_stopped;
    }
    if (str == "error") {
        return TaskBase::ts_error;
    }
    if (str == "suspended") {
        return TaskBase::ts_suspended;
    }
    if (str == "suspending") {
        return TaskBase::ts_suspending;
    }
    if (str == "unknown") {
        return TaskBase::ts_unknown;
    }
    assert(false);
    return TaskBase::ts_unknown;
}

void TaskBase::setTaskStatus(TaskStatus stat)
{
    if (stat == ts_finish || stat == ts_stopped) {
        _endTime = TimeUtility::currentTimeInSeconds();
    }
    _propertyMap[TASK_STATUS_PROPERTY_KEY] = taskStatus2Str(stat);
}

void TaskBase::setLastTaskStatus(TaskStatus stat)
{
    _propertyMap[LAST_TASK_STATUS_PROPERTY_KEY] = taskStatus2Str(stat);
}

TaskBase::TaskStatus TaskBase::getTaskStatus() const
{
    const string& taskStatus = getProperty(TASK_STATUS_PROPERTY_KEY);
    return str2TaskStatus(taskStatus);
}

TaskBase::TaskStatus TaskBase::getLastTaskStatus() const
{
    const string& taskStatus = getProperty(LAST_TASK_STATUS_PROPERTY_KEY);
    return str2TaskStatus(taskStatus);
}

bool TaskBase::isTaskFinish() const { return getTaskStatus() == ts_finish; }

bool TaskBase::isTaskFinishing() const { return getTaskStatus() == ts_finishing; }

bool TaskBase::isTaskStopped() const { return getTaskStatus() == ts_stopped; }

bool TaskBase::isTaskStopping() const { return getTaskStatus() == ts_stopping; }

bool TaskBase::isTaskRunning() const { return getTaskStatus() == ts_running; }

bool TaskBase::isTaskError() const { return getTaskStatus() == ts_error; }

bool TaskBase::isTaskSuspended() const { return getTaskStatus() == ts_suspended; }

bool TaskBase::isTaskSuspending() const { return getTaskStatus() == ts_suspending; }

TaskBase::TaskMetaPtr TaskBase::createTaskMeta() const
{
    return TaskMetaPtr(new TaskMeta(_taskId, _taskType, _propertyMap, _beginTime, _endTime));
}

bool TaskBase::stop()
{
    auto taskStatus = getTaskStatus();
    switch (taskStatus) {
    case TaskStatus::ts_stopping:
    case TaskStatus::ts_finish:
    case TaskStatus::ts_stopped:
        return true;
    case TaskStatus::ts_init:
    case TaskStatus::ts_finishing:
    case TaskStatus::ts_running:
    case TaskStatus::ts_suspending:
    case TaskStatus::ts_suspended:
        setTaskStatus(TaskStatus::ts_stopping);
        return true;
    default:
        BS_LOG(ERROR, "kill task[%s] fail, TaskStatus[%s].", getIdentifier().c_str(),
               getProperty(TASK_STATUS_PROPERTY_KEY).c_str());
        return false;
    }
}

bool TaskBase::suspend()
{
    auto taskStatus = getTaskStatus();
    setLastTaskStatus(taskStatus);
    switch (taskStatus) {
    case TaskStatus::ts_init:
    case TaskStatus::ts_finish:
    case TaskStatus::ts_stopped:
        setTaskStatus(TaskStatus::ts_suspended);
        return true;
    case TaskStatus::ts_running:
    case TaskStatus::ts_stopping:
    case TaskStatus::ts_finishing:
        if (doSuspend()) {
            setTaskStatus(TaskStatus::ts_suspending);
            return true;
        }
        return false;
    default:
        BS_LOG(ERROR, "suspend task[%s] fail, TaskStatus[%s].", getIdentifier().c_str(),
               getProperty(TASK_STATUS_PROPERTY_KEY).c_str());
        return false;
    }
}

bool TaskBase::doSuspend() { return true; }

bool TaskBase::resume()
{
    if (!isTaskSuspended()) {
        BS_LOG(INFO, "task[%s] is not suspended, no need to resume.", getIdentifier().c_str());
        return true;
    }
    if (doResume()) {
        _propertyMap[TASK_STATUS_PROPERTY_KEY] = taskStatus2Str(getLastTaskStatus());
        return true;
    }
    return false;
}

bool TaskBase::doResume() { return true; }

void TaskBase::accept(const TaskOptimizerPtr& optimizer, TaskOptimizer::OptimizeStep step)
{
    if (!optimizer) {
        return;
    }
    auto taskStatus = getTaskStatus();
    if (taskStatus == TaskBase::ts_running || taskStatus == TaskBase::ts_finishing ||
        taskStatus == TaskBase::ts_stopping || taskStatus == TaskBase::ts_suspending) {
        doAccept(optimizer, step);
    }
}

void TaskBase::doAccept(const TaskOptimizerPtr& optimizer, TaskOptimizer::OptimizeStep step)
{
    assert(false);
    return;
}

void TaskBase::syncTaskProperty(WorkerNodes& workerNodes)
{
    auto taskStatus = getTaskStatus();
    switch (taskStatus) {
    case TaskBase::ts_running:
        doSyncTaskProperty(workerNodes);
        if (isFinished(workerNodes)) {
            setTaskStatus(TaskBase::ts_finish);
        }
        break;
    case TaskBase::ts_finishing:
        doSyncTaskProperty(workerNodes);
        if (isFinished(workerNodes)) {
            setTaskStatus(TaskBase::ts_finish);
        }
        break;
    case TaskBase::ts_suspending:
        if (waitSuspended(workerNodes)) {
            setTaskStatus(TaskBase::ts_suspended);
        }
        break;
    case TaskBase::ts_stopping:
        if (waitSuspended(workerNodes)) {
            workerNodes.clear();
            setTaskStatus(TaskBase::ts_stopped);
        }
        break;
    default:
        workerNodes.clear();
    }

    TaskStatusMetricReporterPtr reporter;
    if (_resourceManager) {
        _resourceManager->getResource(reporter);
    }
    if (reporter) {
        taskStatus = getTaskStatus();
        reporter->reportTaskStatus((int64_t)taskStatus, _kmonTags);
    }
}

bool TaskBase::waitSuspended(WorkerNodes& workerNodes) { return true; }

bool TaskBase::finish(const KeyValueMap& kvMap)
{
    if (doFinish(kvMap)) {
        setTaskStatus(TaskBase::ts_finishing);
        return true;
    }
    return false;
}

bool TaskBase::start(const KeyValueMap& kvMap)
{
    if (doStart(kvMap)) {
        _beginTime = TimeUtility::currentTimeInSeconds();
        _endTime = 0;
        setTaskStatus(TaskBase::ts_running);
        return true;
    }
    return false;
}

bool TaskBase::doStart(const KeyValueMap& kvMap) { return true; }

bool TaskBase::init(const KeyValueMap& kvMap)
{
    if (doInit(kvMap)) {
        setTaskStatus(TaskBase::ts_init);
        return true;
    }
    return false;
}

void TaskBase::filterLabelInfo(KeyValueMap& info, bool isSummary) const
{
    auto isLongText = [](const std::string& key, const std::string& value) { return key.size() + value.size() > 64; };

    auto matchSummary = [](const std::string& key) {
        auto tmpKey = key;
        autil::StringUtil::toLowerCase(tmpKey);
        if (tmpKey.find("status") != string::npos) {
            return true;
        }
        if (tmpKey.find("cluster") != string::npos) {
            return true;
        }
        if (tmpKey.find("step") != string::npos) {
            return true;
        }
        if (tmpKey.find("type") != string::npos) {
            return true;
        }
        if (tmpKey.find("cycle") != string::npos) {
            return true;
        }
        return false;
    };

    for (auto iter = info.begin(); iter != info.end();) {
        if (isLongText(iter->first, iter->second)) {
            info.erase(iter++);
            continue;
        }
        if (isSummary && !matchSummary(iter->first)) {
            info.erase(iter++);
            continue;
        }
        iter++;
    }
}

string TaskBase::getTaskLabelString(KeyValueMap* outputDetailInfo) const
{
    string taskId = "\"" + _taskId + "\"";
    string ret = taskId + "[label=";
    KeyValueMap info;
    info["type"] = _taskType;
    for (auto item : _propertyMap) {
        info[item.first + "_@_property"] = item.second;
    }

    string cycleStr = getDateTsString(_beginTime) + " -> " + getDateTsString(_endTime);
    if (_endTime > _beginTime && _beginTime != 0) {
        int64_t interval = _endTime - _beginTime;
        string intervalStr = StringUtil::toString(interval / 60) + " m" + StringUtil::toString(interval % 60) + " s";
        cycleStr += " = ";
        cycleStr += intervalStr;
    }
    info["task_cycle"] = cycleStr;

    supplementLableInfo(info);
    if (outputDetailInfo) {
        *outputDetailInfo = info;
    }

    filterLabelInfo(info, outputDetailInfo != nullptr);
    ret += "\"";
    ret += EscapeForCCode(string("taskID:") + _taskId + "\n" + ToJsonString(info));
    ret += "\"";

    auto taskStatus = getTaskStatus();
    if (taskStatus == ts_error) {
        ret += ",color=red";
    } else if (taskStatus == ts_running) {
        ret += ",color=green";
    } else if (taskStatus == ts_finishing) {
        ret += ",color=blue";
    } else if (taskStatus == ts_init) {
        ret += ",color=yellow";
    }
    ret += "];\n";
    return ret;
}

string TaskBase::getDateTsString(int64_t tsInSecond) const
{
    if (tsInSecond <= 0) {
        return StringUtil::toString(tsInSecond);
    }

    time_t tmp = tsInSecond;
    struct tm cvt;
    localtime_r(&tmp, &cvt);
    char dataBuf[64];
    size_t len = strftime(dataBuf, 64, "%F %T", &cvt);
    return string(dataBuf, len);
}

void TaskBase::setCycleTime(int64_t beginTime, int64_t endTime)
{
    _beginTime = beginTime;
    _endTime = endTime;
}

}} // namespace build_service::admin
