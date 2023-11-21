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

#include <string>

#include "autil/Lock.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "indexlib/util/counter/CounterMap.h"

namespace build_service { namespace task_base {

class PrepareDataSourceTask : public Task
{
public:
    PrepareDataSourceTask();
    ~PrepareDataSourceTask();

public:
    static const std::string TASK_NAME;

private:
    PrepareDataSourceTask(const PrepareDataSourceTask&);
    PrepareDataSourceTask& operator=(const PrepareDataSourceTask&);

public:
    bool init(TaskInitParam& initParam) override;
    bool handleTarget(const config::TaskTarget& target) override;
    bool isDone(const config::TaskTarget& target) override;

    indexlib::util::CounterMapPtr getCounterMap() override;
    std::string getTaskStatus() override;
    bool hasFatalError() override { return false; }

private:
    mutable autil::RecursiveThreadMutex _lock;
    TaskInitParam _taskInitParam;
    config::TaskTarget _currentFinishTarget;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(PrepareDataSourceTask);

}} // namespace build_service::task_base
