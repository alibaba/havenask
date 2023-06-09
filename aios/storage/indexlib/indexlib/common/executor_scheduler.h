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
#ifndef __INDEXLIB_EXECUTOR_SCHEDULER_H
#define __INDEXLIB_EXECUTOR_SCHEDULER_H

#include <memory>

#include "indexlib/common/executor.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace common {

class ExecutorScheduler
{
public:
    enum SchedulerType { ST_REPEATEDLY, ST_ONCE };

public:
    ExecutorScheduler();
    ~ExecutorScheduler();

public:
    void Add(const ExecutorPtr& executor, SchedulerType type);
    void Execute();

    size_t GetRepeatedlyExecutorsCount() { return mRepeatedlyExecutors.size(); }
    size_t GetOnceExecutorsCount() { return mOnceExecutors.size(); }

private:
    std::vector<ExecutorPtr> mRepeatedlyExecutors;
    std::vector<ExecutorPtr> mOnceExecutors;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ExecutorScheduler);
}} // namespace indexlib::common

#endif //__INDEXLIB_EXECUTOR_SCHEDULER_H
