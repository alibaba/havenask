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

namespace indexlibv2 { namespace table {

struct RealtimeMergeParams {
    // rt-max-segment-count = x：最多有x个segment参与Merge。
    size_t maxSegmentCount = 100;
    // rt-max-total-merge-size = x：最多有x大小的segment参与Merge。单位是M。
    // 边界条件：收集的segment total size大于该配置时，后续的Segment将不再被收集。即：配置非0时总是至少包含1个Segment
    size_t maxTotalMergeSize = (size_t)-1;
};

// 在segment list中从segment
// id最小的segment开始，选择若干个连续的小segment进行合并。segment选取的条件取决于RealtimeMergeParams.
class RealtimeMergeStrategy : public MergeStrategy
{
public:
    RealtimeMergeStrategy() {}
    ~RealtimeMergeStrategy() {}

public:
    std::string GetName() const override { return MergeStrategyDefine::REALTIME_MERGE_STRATEGY_NAME; }
    std::pair<Status, std::shared_ptr<MergePlan>> CreateMergePlan(const framework::IndexTaskContext* context) override;
    std::pair<Status, std::shared_ptr<MergePlan>> DoCreateMergePlan(const framework::IndexTaskContext* context);

public:
    static std::pair<Status, struct RealtimeMergeParams> ExtractParams(const config::MergeStrategyParameter& param);

private:
    std::vector<std::shared_ptr<framework::Segment>>
    CollectSrcSegments(const std::shared_ptr<framework::TabletData>& tabletData);

private:
    struct RealtimeMergeParams _params;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
