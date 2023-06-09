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

// 当启动合并时，默认所有的segment将被合并成一个segment。但是下列参数可以控制参与merge的segment的资格，和输出segment的个数。
struct OptimizeMergeParams {
    static const uint32_t DEFAULT_MAX_DOC_COUNT = std::numeric_limits<uint32_t>::max();
    static const uint64_t DEFAULT_AFTER_MERGE_MAX_DOC_COUNT = std::numeric_limits<uint64_t>::max();
    static const uint32_t INVALID_AFTER_MERGE_MAX_SEGMENT_COUNT = std::numeric_limits<uint32_t>::max();

    // max-doc-count: 标识参与 merge 的 segment 的最大doc数，只有小于等于 max-doc-count 的 segment 才会参与 merge。
    // 例如：segment_0 为全量 build 产生，文档数为 31000000，那么配置 max-doc-count = 30000000，segment_0 就不会参与
    // merge。
    uint32_t maxDocCount = DEFAULT_MAX_DOC_COUNT;
    //  after-merge-max-doc-count: 标识多个 segment 进行 merge 之后的最小 doc 数。 例如： segment_0 共有 5 篇 doc,
    //  segment_1 共有 6 篇 doc, segment_2 共有 7 篇 doc, 设置 after-merge-max-doc-count = 10, 则 segment_0 和
    //  segment_1 会 merge 成 1 个 segment, segment_2 会 merge 成一个 segment。不支持带有 truncate 和 adaptive
    //  bitmap 的索引。
    uint64_t afterMergeMaxDocCount = DEFAULT_AFTER_MERGE_MAX_DOC_COUNT;
    //  after-merge-max-segment-count : 标识 merge 之后最多有几个 segment。 我们会先算出总共的 doc
    //  数量，之后根据配置算出 merge 之后每个 segment预期会有多少篇 doc, 之后从前到后尽量均匀分配。不支持带有 truncate
    //  和 adaptive bitmap 的索引。
    uint32_t afterMergeMaxSegmentCount = INVALID_AFTER_MERGE_MAX_SEGMENT_COUNT;
    // "skip-single-merged-segment"
    bool skipSingleMergedSegment = true;
};

class OptimizeMergeStrategy : public MergeStrategy
{
public:
    OptimizeMergeStrategy();
    ~OptimizeMergeStrategy();

public:
    std::string GetName() const override { return MergeStrategyDefine::OPTIMIZE_MERGE_STRATEGY_NAME; }

    std::pair<Status, std::shared_ptr<MergePlan>> CreateMergePlan(const framework::IndexTaskContext* context) override;

public:
    static std::pair<Status, struct OptimizeMergeParams> ExtractParams(const config::MergeStrategyParameter& param);

private:
    std::pair<Status, int64_t> SplitDocCount(const std::vector<std::shared_ptr<framework::Segment>>& srcSegments) const;
    std::pair<Status, std::shared_ptr<MergePlan>>
    DoCreateMergePlan(const framework::IndexTaskContext* context,
                      const std::vector<std::shared_ptr<framework::Segment>>& srcSegments);
    std::vector<std::shared_ptr<framework::Segment>>
    CollectSrcSegments(const std::shared_ptr<framework::TabletData>& tabletData, bool optimize);
    bool NeedMerge(const std::shared_ptr<framework::TabletData>& tabletData,
                   const std::shared_ptr<MergePlan>& plans) const;

private:
    OptimizeMergeParams _params;

private:
    friend class OptimizeMergeStrategyTest;
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
