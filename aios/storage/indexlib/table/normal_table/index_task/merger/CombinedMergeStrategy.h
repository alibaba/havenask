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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/index_task/merger/MergeStrategy.h"
#include "indexlib/table/index_task/merger/MergeStrategyDefine.h"
#include "indexlib/table/index_task/merger/RealtimeMergeStrategy.h"

namespace indexlibv2 { namespace table {

// 同时用realtime和priority queue的merge策略生成merge plan.
// realtime merge策略用于处理build segment，priority queue merge策略用于处理merged segment.

// realtime merge strategy related parameters can be set in one of InputLimits, strategyConditions or OutputLimits.
// realtime merge strategy related parameters must be set and cannot be omitted.
// priority queue merge strategy related parameters can be omitted. If set, they can be joined together with realtime
// merge strategy related parameters.
// The same MergeStrategyParamters is used in both realtime and priority queue merge strategies.
// See unit tests for param examples.
// Configurations for priority queue merge strategy will not affect realtime merge strategy, this also applies vice
// versa.
class CombinedMergeStrategy : public MergeStrategy
{
public:
    explicit CombinedMergeStrategy(std::unique_ptr<MergeStrategy> mergeStrategy)
        : _mergedSegmentStrategy(std::move(mergeStrategy))
    {
    }
    ~CombinedMergeStrategy() = default;

public:
    std::string GetName() const override { return MergeStrategyDefine::COMBINED_MERGE_STRATEGY_NAME; }
    std::pair<Status, std::shared_ptr<MergePlan>> CreateMergePlan(const framework::IndexTaskContext* context) override;

private:
    std::pair<Status, std::shared_ptr<MergePlan>>
    GetMergedSegmentMergePlan(const framework::IndexTaskContext* context) const;
    Status FullFillSegmentMergePlan(const std::shared_ptr<framework::TabletData>& tabletData,
                                    segmentid_t* targetSegmentId, SegmentMergePlan* segmentMergePlan);

private:
    std::unique_ptr<MergeStrategy> _mergedSegmentStrategy;
    std::unique_ptr<RealtimeMergeStrategy> _buildSegmentStrategy;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
