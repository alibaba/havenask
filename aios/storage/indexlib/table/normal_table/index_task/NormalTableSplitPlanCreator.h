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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/table/normal_table/index_task/NormalTableCompactPlanCreator.h"
namespace indexlibv2::table {

class NormalTableSplitPlanCreator : public NormalTableCompactPlanCreator
{
public:
    NormalTableSplitPlanCreator(const std::string& taskName);
    ~NormalTableSplitPlanCreator();

public:
    std::pair<std::string, std::unique_ptr<MergeStrategy>> CreateCompactStrategy(const std::string& mergeStrategyName,
                                                                                 bool isOptimizedCompact) override;
    std::pair<Status, bool> NeedTriggerTask(const config::IndexTaskConfig& taskConfig,
                                            const framework::IndexTaskContext* taskContext) const override;

    static Status AppendDefaultConfigIfNecessary(const framework::IndexTaskContext* taskContext,
                                                 std::vector<config::IndexTaskConfig>* configs);

public:
    static const std::string TASK_TYPE;

private:
    static const std::string DEFAULT_TASK_NAME;
    static const std::string DEFAULT_TASK_PERIOD;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
