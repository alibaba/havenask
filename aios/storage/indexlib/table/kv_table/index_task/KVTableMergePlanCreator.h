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
#include "indexlib/base/Status.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"
#include "indexlib/table/index_task/SimpleIndexTaskPlanCreator.h"
#include "indexlib/table/index_task/merger/MergePlan.h"

namespace indexlibv2::config {
class MergeConfig;
}

namespace indexlibv2::framework {
class IndexTaskContext;
class Version;
class TabletData;
} // namespace indexlibv2::framework

namespace indexlibv2::table {
class MergeStrategy;
class ReclaimMap;

class KVTableMergePlanCreator : public SimpleIndexTaskPlanCreator
{
public:
    KVTableMergePlanCreator(const std::string& taskName);
    ~KVTableMergePlanCreator();

public:
    std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
    CreateTaskPlan(const framework::IndexTaskContext* taskContext) override;

private:
    std::string ConstructLogTaskType() const override;
    std::string ConstructLogTaskId(const framework::IndexTaskContext* taskContext,
                                   const framework::Version& targetVersion) const override;

    // rewrite mergeConfig in taskContext from IndexTaskConfig
    // otherwise merge strategy should change logic, any better way ?
    Status RewriteMergeConfig(const framework::IndexTaskContext* taskContext);

    std::unique_ptr<MergeStrategy> CreateMergeStrategy(const framework::IndexTaskContext* taskContext);
    Status CommitMergePlans(const std::shared_ptr<MergePlan>& mergePlan,
                            const framework::IndexTaskContext* taskContext);

public:
    static const std::string TASK_TYPE;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
