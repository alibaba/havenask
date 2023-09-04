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
#ifndef ISEARCH_BS_RUNSCRIPTTASK_H
#define ISEARCH_BS_RUNSCRIPTTASK_H

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "build_service_tasks/script_task/ScriptExecutor.h"

namespace build_service { namespace task_base {

class RunScriptTask : public Task
{
public:
    RunScriptTask();
    ~RunScriptTask();

public:
    static const std::string TASK_NAME;

private:
    RunScriptTask(const RunScriptTask&);
    RunScriptTask& operator=(const RunScriptTask&);

public:
    bool init(TaskInitParam& initParam) override;
    bool handleTarget(const config::TaskTarget& target) override;
    bool isDone(const config::TaskTarget& target) override;

    indexlib::util::CounterMapPtr getCounterMap() override;
    std::string getTaskStatus() override;

    int64_t getRetryTimes() const;
    bool hasFatalError() override { return false; }

private:
    void setLatestTarget(const config::TaskTarget& target);
    void setBSEnv(const std::string& logRootDir);
    std::string toInstanceInfoStr(const Task::InstanceInfo& info);
    config::TaskTarget getLatestTarget() const;
    void workThread();

    void TEST_setDebugMode(bool debugMode) { _debugMode = debugMode; }
    bool TEST_isHanged() { return _executor && _executor->isHanged(); };

private:
    mutable autil::RecursiveThreadMutex _lock;
    TaskInitParam _taskInitParam;
    config::TaskTarget _currentFinishTarget;
    config::TaskTarget _latestTarget;
    ScriptExecutorPtr _executor;
    autil::LoopThreadPtr _loopThreadPtr;

    bool _debugMode; // for test
private:
    friend class RunScriptTaskTest;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RunScriptTask);

}} // namespace build_service::task_base

#endif // ISEARCH_BS_RUNSCRIPTTASK_H
