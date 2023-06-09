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
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/table/index_task/SimpleIndexTaskPlanCreator.h"

namespace indexlibv2::framework {
class Version;
}
namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlibv2::table {

class NormalTableAlterTablePlanCreator : public SimpleIndexTaskPlanCreator
{
public:
    NormalTableAlterTablePlanCreator(const std::string& taskName);
    ~NormalTableAlterTablePlanCreator();

public:
    std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
    CreateTaskPlan(const framework::IndexTaskContext* taskContext) override;

private:
    std::string ConstructLogTaskType() const override;
    std::string ConstructLogTaskId(const framework::IndexTaskContext* taskContext,
                                   const framework::Version& targetVersion) const override;

    void CalculateAlterIndexConfigs(const std::shared_ptr<framework::Segment>& segment, schemaid_t baseSchemaId,
                                    schemaid_t targetSchemaId, const std::shared_ptr<framework::TabletData>& tabletData,
                                    std::vector<std::shared_ptr<config::IIndexConfig>>& addIndexConfigs,
                                    std::vector<std::shared_ptr<config::IIndexConfig>>& deleteIndexConfigs) const;
    bool UseDefaultValueStrategy(schemaid_t baseSchemaId, schemaid_t targetSchemaId,
                                 const std::shared_ptr<framework::TabletData>& tabletData) const;

    std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
    CreateDefaultValueTaskPlan(const framework::Version& targetVersion);

    bool UnsupportIndexType(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig) const;

public:
    static const std::string TASK_TYPE;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
