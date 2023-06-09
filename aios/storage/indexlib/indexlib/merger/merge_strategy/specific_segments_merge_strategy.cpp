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
#include "indexlib/merger/merge_strategy/specific_segments_merge_strategy.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, SpecificSegmentsMergeStrategy);

SpecificSegmentsMergeStrategy::SpecificSegmentsMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                                                             const IndexPartitionSchemaPtr& schema)
    : MergeStrategy(segDir, schema)
{
}

SpecificSegmentsMergeStrategy::~SpecificSegmentsMergeStrategy() {}

void SpecificSegmentsMergeStrategy::SetParameter(const MergeStrategyParameter& param)
{
    string mergeParam = param.GetLegacyString();
    StringUtil::trim(mergeParam);
    if (mergeParam.empty()) {
        return;
    }
    StringTokenizer st(mergeParam, "=", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() != 2 || st[0] != "merge_segments") {
        INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]", mergeParam.c_str());
    }
    StringUtil::fromString(st[1], mTargetSegmentIds, ",", ";");
}

std::string SpecificSegmentsMergeStrategy::GetParameter() const
{
    if (mTargetSegmentIds.empty()) {
        return "";
    }

    stringstream ss;
    ss << "merge_segments=";
    for (size_t i = 0; i < mTargetSegmentIds.size(); i++) {
        for (size_t j = 0; j < mTargetSegmentIds[i].size(); ++j) {
            ss << mTargetSegmentIds[i][j];
            if (j != mTargetSegmentIds[i].size() - 1) {
                ss << ",";
            }
        }
        if (i != mTargetSegmentIds.size() - 1) {
            ss << ";";
        }
    }
    return ss.str();
}

MergeTask SpecificSegmentsMergeStrategy::CreateMergeTask(const SegmentMergeInfos& segMergeInfos,
                                                         const indexlibv2::framework::LevelInfo& levelInfo)
{
    MergeTask task;
    assert(segMergeInfos.size() > 0);
    for (size_t i = 0; i < mTargetSegmentIds.size(); ++i) {
        MergePlan plan;
        for (size_t j = 0; j < mTargetSegmentIds[i].size(); ++j) {
            segmentid_t segId = mTargetSegmentIds[i][j];
            SegmentMergeInfo segMergeInfo;
            if (GetSegmentMergeInfo(segId, segMergeInfos, segMergeInfo)) {
                plan.AddSegment(segMergeInfo);
            }
        }
        if (!plan.Empty()) {
            IE_LOG(INFO, "Merge plan generated [%s] by specific segments", plan.ToString().c_str());
            task.AddPlan(plan);
        }
    }
    return task;
}

MergeTask SpecificSegmentsMergeStrategy::CreateMergeTaskForOptimize(const SegmentMergeInfos& segMergeInfos,
                                                                    const indexlibv2::framework::LevelInfo& levelInfo)
{
    OptimizeMergeStrategy strategy(mSegDir, mSchema);
    return strategy.CreateMergeTask(segMergeInfos, levelInfo);
}

bool SpecificSegmentsMergeStrategy::GetSegmentMergeInfo(segmentid_t segmentId, const SegmentMergeInfos& segMergeInfos,
                                                        SegmentMergeInfo& segMergeInfo)
{
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        if (segmentId == segMergeInfos[i].segmentId) {
            segMergeInfo = segMergeInfos[i];
            return true;
        }
    }
    return false;
}
}} // namespace indexlib::merger
