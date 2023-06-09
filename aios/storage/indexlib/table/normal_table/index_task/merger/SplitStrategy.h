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
#include "indexlib/table/index_task/merger/MergeStrategy.h"
#include "indexlib/table/index_task/merger/MergeStrategyDefine.h"
#include "indexlib/table/normal_table/index_task/merger/NormalTabletMergeStrategyDefine.h"

namespace indexlibv2::table {
class SegmentGroupConfig;
class SingleSegmentDocumentGroupSelector;
class SplitStrategy : public MergeStrategy
{
public:
    SplitStrategy();
    virtual ~SplitStrategy();

public:
    std::string GetName() const override { return MergeStrategyDefine::SPLIT_STRATEGY_NAME; }
    std::pair<Status, std::shared_ptr<MergePlan>> CreateMergePlan(const framework::IndexTaskContext* context) override;

private:
    Status InitFromContext(const framework::IndexTaskContext* context);
    Status CalculateSegmentStatistics(const framework::IndexTaskContext* context,
                                      std::vector<std::pair<double, segmentid_t>>* segmentPriority,
                                      std::map<segmentid_t, std::vector<int32_t>>* segmentGroupDocCounts);
    // virtual for test
    virtual std::pair<Status, std::vector<int32_t>> CalculateGroupCounts(const framework::IndexTaskContext* context,
                                                                         std::shared_ptr<framework::Segment> segment);

private:
    OutputLimits _outputLimits;
    std::shared_ptr<SegmentGroupConfig> _segmentGroupConfig;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
