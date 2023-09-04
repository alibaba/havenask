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
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/index_task/merger/MergeStrategy.h"
#include "indexlib/table/index_task/merger/MergeStrategyDefine.h"
#include "indexlib/table/index_task/merger/RealtimeMergeStrategy.h"

namespace indexlibv2::table {

// 自适应merge策略，目标是在用户无需配置任何参数的前提下，自适应地调整merge的IO量，尽可能的使segment数量、deleted doc
// count、文档总数保持在健康的水位上
class AdaptiveMergeStrategy : public MergeStrategy
{
public:
    AdaptiveMergeStrategy();
    ~AdaptiveMergeStrategy();

public:
    std::string GetName() const override { return MergeStrategyDefine::ADAPTIVE_MERGE_STRATEGY_NAME; }
    std::pair<Status, std::shared_ptr<MergePlan>> CreateMergePlan(const framework::IndexTaskContext* context) override;

private:
    std::pair<Status, std::shared_ptr<MergePlan>> DoCreateMergePlan(const framework::IndexTaskContext* context);
    size_t GetValidSegmentSize(size_t docCount, size_t deleteDocCount, size_t segmentSize);

    std::vector<segmentid_t>
    FigureOutFinalCandidates(std::vector<std::tuple<segmentid_t, size_t, size_t>> estimateValues);

private:
    std::unique_ptr<RealtimeMergeStrategy> _realtimeMergeStrategy;

    constexpr static size_t DOC_BASE_LINE = 1 << 21;
    constexpr static size_t EMERGENCY_SPACE = 1 << 28;
    constexpr static size_t DOC_DEAD_LINE = std::numeric_limits<docid_t>::max();
    constexpr static size_t QUOTA_AS_IDEAL_SEGMEN_COUNT = 2;
    constexpr static size_t DYNAMIC_PROGRAMMING_SEGMENT_LIMIT = 64;
    constexpr static size_t DYNAMIC_PROGRAMMING_NORMALIZED_COST = 1000;
    constexpr static double EMERGENCY_DELETED_DOC_RATIO = 0.2;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
