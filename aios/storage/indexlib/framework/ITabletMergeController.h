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

#include <optional>

#include "autil/NoCopyable.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/index_task/IIndexTaskPlanCreator.h"
#include "indexlib/framework/index_task/MergeTaskDefine.h"

namespace indexlibv2::framework {

class ITabletMergeController : private autil::NoCopyable
{
public:
    ITabletMergeController() = default;
    virtual ~ITabletMergeController() = default;

    struct TaskStat {
        versionid_t baseVersionId = INVALID_VERSIONID;
        int64_t finishedOpCount = -1;
        int64_t totalOpCount = -1;
    };

public:
    virtual future_lite::coro::Lazy<Status> Recover() = 0;
    virtual std::optional<TaskStat> GetRunningTaskStat() const = 0;
    virtual future_lite::coro::Lazy<std::pair<Status, versionid_t>> GetLastMergeTaskResult() = 0;
    virtual std::unique_ptr<IndexTaskContext> CreateTaskContext(versionid_t baseVersionId, const std::string& taskType,
                                                                const std::string& taskName,
                                                                const std::map<std::string, std::string>& params) = 0;
    virtual future_lite::coro::Lazy<Status> SubmitMergeTask(std::unique_ptr<IndexTaskPlan> plan,
                                                            IndexTaskContext* context) = 0;

    virtual future_lite::coro::Lazy<std::pair<Status, MergeTaskStatus>> WaitMergeResult() = 0;
    virtual Status CleanTask(bool removeTempFiles) = 0;
    virtual future_lite::coro::Lazy<Status> CancelCurrentTask() = 0;
    virtual void Stop() = 0;
};

} // namespace indexlibv2::framework
