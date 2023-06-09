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
#include "indexlib/merger/merge_strategy/segment_count_merge_strategy.h"

#include <iomanip>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, SegmentCountMergeStrategy);

SegmentCountMergeStrategy::SegmentCountMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                                                     const IndexPartitionSchemaPtr& schema)
    : MergeStrategy(segDir, schema)
    , mSegmentCount(3)
{
}

SegmentCountMergeStrategy::~SegmentCountMergeStrategy() {}

void SegmentCountMergeStrategy::SetParameter(const MergeStrategyParameter& param)
{
    if (!param.inputLimitParam.empty() || !param.strategyConditions.empty()) {
        INDEXLIB_THROW(util::BadParameterException, "Not support input_limits and strategy_conditions param for %s",
                       SEGMENT_COUNT_MERGE_STRATEGY_STR);
    }

    if (param.outputLimitParam.empty()) {
        IE_LOG(INFO, "Use default param for SegmentCountMergeStragy, target segment count [%u]", mSegmentCount);
        return;
    }

    StringTokenizer st(param.outputLimitParam, "=", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() != 2 || st[0] != "segment-count") {
        INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]",
                       param.outputLimitParam.c_str());
    }

    if (!StringUtil::strToUInt32(st[1].c_str(), mSegmentCount)) {
        INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]",
                       param.outputLimitParam.c_str());
    }
}

string SegmentCountMergeStrategy::GetParameter() const
{
    stringstream ss;
    ss << "segment-count=" << mSegmentCount;
    return ss.str();
}

MergeTask SegmentCountMergeStrategy::CreateMergeTask(const SegmentMergeInfos& segMergeInfos,
                                                     const indexlibv2::framework::LevelInfo& levelInfo)
{
    indexlibv2::framework::LevelTopology topology = levelInfo.GetTopology();
    if (topology != indexlibv2::framework::topo_sequence) {
        INDEXLIB_FATAL_ERROR(UnSupported, " [%s] is unsupported, only support sequence",
                             indexlibv2::framework::LevelMeta::TopologyToStr(topology).c_str());
    }

    if (segMergeInfos.empty()) {
        IE_LOG(INFO, "No segment, no need merge");
        return MergeTask();
    }

    MergePlan mergePlan = CreateMergePlan(segMergeInfos);
    IE_LOG(INFO, "Merge plan generated [%s]", mergePlan.ToString().c_str());
    MergeTask mergeTask;
    mergeTask.AddPlan(mergePlan);
    return mergeTask;
}

MergePlan SegmentCountMergeStrategy::CreateMergePlan(const SegmentMergeInfos& segMergeInfos)
{
    MergePlan mergePlan;
    if (segMergeInfos.size() <= mSegmentCount) {
        auto iter = std::max_element(segMergeInfos.begin(), segMergeInfos.end(),
                                     [](const SegmentMergeInfo& largest, const SegmentMergeInfo& item) {
                                         if (largest.deletedDocCount < item.deletedDocCount) {
                                             return largest.segmentInfo.docCount < item.segmentInfo.docCount;
                                         }
                                         return largest.deletedDocCount < item.deletedDocCount;
                                     });
        mergePlan.AddSegment(*iter);
        return mergePlan;
    }

    SegmentMergeInfos sortedSegMergeInfos = segMergeInfos;
    std::sort(sortedSegMergeInfos.begin(), sortedSegMergeInfos.end(),
              [](const SegmentMergeInfo& left, const SegmentMergeInfo& right) {
                  if (left.segmentInfo.mergedSegment == right.segmentInfo.mergedSegment) {
                      return left.segmentSize * (1 - 1.0 * left.deletedDocCount / left.segmentInfo.docCount) >
                             right.segmentSize * (1 - 1.0 * right.deletedDocCount / right.segmentInfo.docCount);
                  }
                  return left.segmentInfo.mergedSegment;
              });
    std::stringstream ss;
    ss << setiosflags(ios::fixed) << setprecision(1);
    for (const SegmentMergeInfo& info : sortedSegMergeInfos) {
        ss << info.segmentId << "(" << (info.segmentInfo.mergedSegment ? "Merged," : "")
           << (info.segmentSize / 1024.0 / 1024 / 1024) << "G*"
           << 100 * (1 - 1.0 * info.deletedDocCount / info.segmentInfo.docCount) << "%; ";
    }
    IE_LOG(INFO, "select last [%zu] segment from sorted segment merge infos [%s]",
           sortedSegMergeInfos.size() - mSegmentCount + 1, ss.str().c_str());
    for (size_t i = mSegmentCount - 1; i < sortedSegMergeInfos.size(); ++i) {
        mergePlan.AddSegment(sortedSegMergeInfos[i]);
    }
    return mergePlan;
}

MergeTask SegmentCountMergeStrategy::CreateMergeTaskForOptimize(const SegmentMergeInfos& segMergeInfos,
                                                                const indexlibv2::framework::LevelInfo& levelInfo)
{
    OptimizeMergeStrategy strategy(mSegDir, mSchema);
    return strategy.CreateMergeTask(segMergeInfos, levelInfo);
}
}} // namespace indexlib::merger
