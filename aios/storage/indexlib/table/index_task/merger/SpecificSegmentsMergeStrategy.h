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
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/index_task/merger/MergeStrategy.h"
#include "indexlib/table/index_task/merger/MergeStrategyDefine.h"

namespace indexlibv2::table {

class SpecificSegmentsMergeStrategy : public MergeStrategy
{
public:
    SpecificSegmentsMergeStrategy() {};
    ~SpecificSegmentsMergeStrategy() {};

public:
    std::string GetName() const override { return MergeStrategyDefine::SPECIFIC_SEGMENTS_MERGE_STRATEGY_NAME; }

    std::pair<Status, std::shared_ptr<MergePlan>> CreateMergePlan(const framework::IndexTaskContext* context) override;

private:
    std::pair<Status, std::vector<segmentid_t>> ExtractParams(const config::MergeStrategyParameter& param);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
