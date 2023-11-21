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
#include "build_service_tasks/script_task/RunScriptTask.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <map>
#include <unistd.h>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace build_service::util;
using namespace build_service::config;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, RunScriptTask);

const string RunScriptTask::TASK_NAME = BS_TASK_RUN_SCRIPT;

RunScriptTask::RunScriptTask() : _debugMode(false) { _executor.reset(new ScriptExecutor()); }

RunScriptTask::~RunScriptTask()
{
    if (_loopThreadPtr) {
        _loopThreadPtr->stop();
        _loopThreadPtr.reset();
    }
}

bool RunScriptTask::init(TaskInitParam& initParam)
{
    _taskInitParam = initParam;
    char cwdPath[1024];
    if (getcwd(cwdPath, sizeof(cwdPath)) == NULL) {
        BS_LOG(ERROR, "getcwd fail!");
        return false;
        // if workdir is too long, getcwd will return NULL.
    }
    auto resourceReader = _taskInitParam.resourceReader;
    string scriptArgsFile = ResourceReader::getTaskScriptFileRelativePath("script_control_args.json");
    string logRootDir;
    string content;
    if (resourceReader->getConfigContent(scriptArgsFile, content)) {
        KeyValueMap scriptArgs;
        try {
            autil::legacy::FromJsonString(scriptArgs, content);
        } catch (const autil::legacy::ExceptionBase& e) {
            string errorMsg =
                "parse script_control_args.json file from [" + content + "] failed, exception[" + e.what() + "]";
            REPORT_ERROR_WITH_ADVICE(proto::SERVICE_TASK_ERROR, errorMsg, proto::BS_STOP);
            return false;
        }
        logRootDir = scriptArgs["log_dir"];
    } else {
        logRootDir = string(cwdPath);
    }

    if (!_executor->init(logRootDir)) {
        return false;
    }

    // set env from set_env.json if file exist
    string relativeFilePath = ResourceReader::getTaskScriptFileRelativePath("set_env.json");
    content.clear();
    if (resourceReader->getConfigContent(relativeFilePath, content)) {
        KeyValueMap customizeEnvmap;
        try {
            autil::legacy::FromJsonString(customizeEnvmap, content);
        } catch (const autil::legacy::ExceptionBase& e) {
            BS_LOG(ERROR, "parse set_env.json file from [%s] failed, exception[%s]", content.c_str(), e.what());
            return false;
        }
        for (const auto& item : customizeEnvmap) {
            _executor->setEnv(item.first, item.second);
        }
    }
    setBSEnv(logRootDir);
    if (_debugMode) {
        return true;
    }
    _loopThreadPtr = LoopThread::createLoopThread(bind(&RunScriptTask::workThread, this), 1000 * 1000);
    return _loopThreadPtr != NULL;
}

void RunScriptTask::setBSEnv(const string& logRootDir)
{
    _executor->setEnv("BS_TASK_INSTANCE_INFO", toInstanceInfoStr(_taskInitParam.instanceInfo));
    _executor->setEnv("BS_TASK_PARTITION_ID", StringUtil::toString(_taskInitParam.instanceInfo.partitionId));
    _executor->setEnv("BS_TASK_INSTANCE_ID", StringUtil::toString(_taskInitParam.instanceInfo.instanceId));
    _executor->setEnv("BS_TASK_LOG_ROOT_DIR", logRootDir);
}

bool RunScriptTask::handleTarget(const TaskTarget& target)
{
    if (isDone(target)) {
        return true;
    }
    setLatestTarget(target);

    int64_t ts = _executor->getBeginRunCmdTimeInSecond();
    if (ts < 0) {
        return true;
    }

    int64_t gap = TimeUtility::currentTimeInSeconds() - ts;
    if (gap > 10) {
        BS_INTERVAL_LOG2(10, WARN,
                         "script already running over [%ld] seconds, "
                         "dead loop may exist in script!",
                         gap);
    }
    return true;
}

void RunScriptTask::workThread()
{
    TaskTarget target = getLatestTarget();
    if (isDone(target)) {
        return;
    }

    string runScriptStr;
    if (!target.getTargetDescription("runScript", runScriptStr)) {
        BS_INTERVAL_LOG2(10, ERROR, "fail to get runScript from target [%s]", ToJsonString(target).c_str());
        return;
    }

    auto resourceReader = _taskInitParam.resourceReader;
    string relativePath = resourceReader->getTaskScriptFileRelativePath("");
    string scriptRoot = fslib::util::FileUtil::joinFilePath(resourceReader->getConfigPath(), relativePath);
    string errorMsg;
    proto::ErrorAdvice advice;
    bool finished = _executor->runScript(scriptRoot, runScriptStr, &errorMsg, &advice);
    if (!errorMsg.empty()) {
        REPORT_ERROR_WITH_ADVICE(proto::SERVICE_TASK_ERROR, errorMsg, advice);
    }

    if (!finished) {
        if (_executor->isHanged()) {
            // write log every 1h
            BS_INTERVAL_LOG2(3600, ERROR, "runScript [%s] isHanged", runScriptStr.c_str());
        } else {
            BS_INTERVAL_LOG2(10, INFO, "runScript [%s] will retry", runScriptStr.c_str());
        }
    } else {
        ScopedLock lock(_lock);
        _currentFinishTarget = target;
        BS_LOG(INFO, "runScript [%s] finish !", runScriptStr.c_str());
    }
    return;
}

bool RunScriptTask::isDone(const TaskTarget& targetDescription)
{
    ScopedLock lock(_lock);
    if (targetDescription == _currentFinishTarget) {
        return true;
    }
    return false;
}

indexlib::util::CounterMapPtr RunScriptTask::getCounterMap() { return indexlib::util::CounterMapPtr(); }

std::string RunScriptTask::getTaskStatus() { return "run script task is running"; }

void RunScriptTask::setLatestTarget(const TaskTarget& target)
{
    ScopedLock lock(_lock);
    _latestTarget = target;
}

TaskTarget RunScriptTask::getLatestTarget() const
{
    ScopedLock lock(_lock);
    return _latestTarget;
}

string RunScriptTask::toInstanceInfoStr(const Task::InstanceInfo& info)
{
    vector<string> tmp;
    tmp.push_back(string("partitionCount=") + StringUtil::toString(info.partitionCount));
    tmp.push_back(string("partitionId=") + StringUtil::toString(info.partitionId));
    tmp.push_back(string("instanceCount=") + StringUtil::toString(info.instanceCount));
    tmp.push_back(string("instanceId=") + StringUtil::toString(info.instanceId));
    return StringUtil::toString(tmp, ",");
}

int64_t RunScriptTask::getRetryTimes() const
{
    int64_t retryTimes = _executor->getRetryTimes();
    return retryTimes < 0 ? 0 : retryTimes;
}

}} // namespace build_service::task_base
