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
#include "indexlib/common/executor_scheduler.h"

using namespace std;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, ExecutorScheduler);

ExecutorScheduler::ExecutorScheduler() {}

ExecutorScheduler::~ExecutorScheduler() {}

void ExecutorScheduler::Add(const ExecutorPtr& executor, SchedulerType type)
{
    if (type == ST_REPEATEDLY) {
        mRepeatedlyExecutors.push_back(executor);
    } else if (type == ST_ONCE) {
        mOnceExecutors.push_back(executor);
    }
}

void ExecutorScheduler::Execute()
{
    for (size_t i = 0; i < mRepeatedlyExecutors.size(); ++i) {
        mRepeatedlyExecutors[i]->Execute();
    }

    for (size_t i = 0; i < mOnceExecutors.size(); ++i) {
        mOnceExecutors[i]->Execute();
    }
    mOnceExecutors.clear();
}
}} // namespace indexlib::common
