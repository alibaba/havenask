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
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/merger/parallel_partition_data_merger.h"

namespace build_service { namespace task_base {

class EndBuildTask : public Task
{
public:
    EndBuildTask();
    virtual ~EndBuildTask();

public:
    static const std::string TASK_NAME;
    static const std::string CHECKPOINT_NAME;

public:
    bool init(Task::TaskInitParam& initParam) override;
    bool handleTarget(const config::TaskTarget& target) override;
    bool isDone(const config::TaskTarget& target) override;
    indexlib::util::CounterMapPtr getCounterMap() override;
    std::string getTaskStatus() override;
    bool hasFatalError() override { return false; }

private:
    bool prepareParallelBuildInfo(const config::TaskTarget& target,
                                  indexlib::index_base::ParallelBuildInfo& buildInfo) const;
    void prepareIndexOptions(const config::TaskTarget& target);

private:
    mutable autil::RecursiveThreadMutex _lock;
    volatile bool _finished;
    Task::TaskInitParam _taskInitParam;
    KeyValueMap _kvMap;
    indexlib::config::IndexPartitionOptions _options;
    config::TaskTarget _currentFinishTarget;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(EndBuildTask);

}} // namespace build_service::task_base
