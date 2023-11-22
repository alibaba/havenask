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

#include <memory>
#include <stdint.h>
#include <vector>

#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/table/merge_task_description.h"
#include "indexlib/table/task_execute_meta.h"

namespace indexlib { namespace table {

class MergeTaskDispatcher
{
public:
    MergeTaskDispatcher();
    virtual ~MergeTaskDispatcher();

public:
    virtual std::vector<TaskExecuteMetas> DispatchMergeTasks(const std::vector<MergeTaskDescriptions>& taskDescriptions,
                                                             uint32_t instanceCount);

private:
    class WorkLoadCounter
    {
    public:
        WorkLoadCounter(TaskExecuteMetas& _taskMetas, uint64_t _workLoad) : taskMetas(_taskMetas), workLoad(_workLoad)
        {
        }
        ~WorkLoadCounter() {}

    public:
        TaskExecuteMetas& taskMetas;
        uint64_t workLoad;
    };
    typedef std::shared_ptr<WorkLoadCounter> WorkLoadCounterPtr;
    class WorkLoadComp
    {
    public:
        bool operator()(const WorkLoadCounterPtr& lhs, const WorkLoadCounterPtr& rhs)
        {
            if (lhs->workLoad == rhs->workLoad) {
                return lhs->taskMetas.size() > rhs->taskMetas.size();
            }
            return lhs->workLoad > rhs->workLoad;
        }
    };

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeTaskDispatcher);
}} // namespace indexlib::table
