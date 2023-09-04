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
#include "indexlib/table/normal_table/index_task/merger/OptimizeMergeStrategy.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/normal_table/index_task/merger/NormalTableMergeStrategyUtil.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace indexlibv2::config;
using namespace indexlib::file_system;

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, OptimizeMergeStrategy);

OptimizeMergeStrategy::OptimizeMergeStrategy() {}

OptimizeMergeStrategy::~OptimizeMergeStrategy() {}

std::pair<Status, std::shared_ptr<MergePlan>>
OptimizeMergeStrategy::CreateMergePlan(const framework::IndexTaskContext* context)
{
    auto tabletData = context->GetTabletData();
    const auto& version = tabletData->GetOnDiskVersion();
    auto segDescs = version.GetSegmentDescriptions();
    auto levelInfo = segDescs->GetLevelInfo();
    if (levelInfo && levelInfo->GetTopology() != framework::topo_sequence) {
        AUTIL_LOG(ERROR, "topology not supported, only support sequence topology");
        return std::make_pair(Status::Corruption("Topology not supported, only support sequence topology"), nullptr);
    }
    auto mergeConfig = context->GetMergeConfig();
    if (mergeConfig.GetMergeStrategyStr() == GetName()) {
        auto [status, params] = ExtractParams(mergeConfig.GetMergeStrategyParameter());
        if (!status.IsOK()) {
            return {status, nullptr};
        }
        _params = std::move(params);
    }
    bool optimize = false;
    (void)context->GetParameter(IS_OPTIMIZE_MERGE, optimize);

    auto srcSegments = CollectSrcSegments(context->GetTabletData(), optimize);
    return DoCreateMergePlan(context, srcSegments);
}

std::pair<Status, struct OptimizeMergeParams> OptimizeMergeStrategy::ExtractParams(const MergeStrategyParameter& param)
{
    struct OptimizeMergeParams params;
    string mergeParam = param.GetLegacyString();
    StringUtil::trim(mergeParam);
    if (mergeParam.empty()) {
        return {Status::OK(), params};
    }

    StringTokenizer st(mergeParam, ";", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (size_t i = 0; i < st.getNumTokens(); i++) {
        StringTokenizer kvStr(st[i], "=", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        if (kvStr.getNumTokens() != 2) {
            auto status = Status::InvalidArgs("invalid parameter [%s] for merge strategy", mergeParam.c_str());
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return {status, params};
        }
        if (kvStr[0] == "max-doc-count") {
            uint32_t maxDocCount;
            if (!StringUtil::strToUInt32(kvStr[1].c_str(), maxDocCount)) {
                auto status = Status::InvalidArgs("invalid parameter [%s] for merge strategy", mergeParam.c_str());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                return {status, params};
            }
            params.maxDocCount = maxDocCount;
        } else if (kvStr[0] == "after-merge-max-doc-count") {
            uint32_t afterMergeMaxDocCount;
            if (!StringUtil::strToUInt32(kvStr[1].c_str(), afterMergeMaxDocCount)) {
                auto status = Status::InvalidArgs("invalid parameter [%s] for merge strategy", mergeParam.c_str());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                return {status, params};
            }
            params.afterMergeMaxDocCount = afterMergeMaxDocCount;
        } else if (kvStr[0] == "after-merge-max-segment-count") {
            uint32_t afterMergeMaxSegmentCount;
            if (!StringUtil::strToUInt32(kvStr[1].c_str(), afterMergeMaxSegmentCount)) {
                auto status = Status::InvalidArgs("invalid parameter [%s] for merge strategy", mergeParam.c_str());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                return {status, params};
            }
            params.afterMergeMaxSegmentCount = afterMergeMaxSegmentCount;
        } else if (kvStr[0] == "skip-single-merged-segment") {
            params.skipSingleMergedSegment = (kvStr[1] != "false");
        } else {
            auto status = Status::InvalidArgs("invalid parameter [%s] for merge strategy", mergeParam.c_str());
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return {status, params};
        }
    }
    if (params.afterMergeMaxSegmentCount != OptimizeMergeParams::INVALID_AFTER_MERGE_MAX_SEGMENT_COUNT &&
        params.afterMergeMaxDocCount != OptimizeMergeParams::DEFAULT_AFTER_MERGE_MAX_DOC_COUNT) {
        auto status = Status::InvalidArgs("invalid parameter [%s] for merge strategy", mergeParam.c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return {status, params};
    }
    // TODO(yijie.zhang): check not support merge to multi segment with truncate and adaptive bitmap
    return {Status::OK(), params};
}

std::pair<Status, int64_t>
OptimizeMergeStrategy::SplitDocCount(const vector<std::shared_ptr<framework::Segment>>& srcSegments) const
{
    uint64_t totalDocCount = 0;
    for (size_t i = 0; i < srcSegments.size(); i++) {
        auto result = NormalTableMergeStrategyUtil::GetDeleteDocCount(srcSegments[i].get());
        if (!result.first.IsOK()) {
            return std::make_pair(result.first, 0);
        }
        totalDocCount += srcSegments[i]->GetSegmentInfo()->docCount - result.second;
    }
    return std::make_pair(Status::OK(), totalDocCount / _params.afterMergeMaxSegmentCount + 1);
}

std::pair<Status, std::shared_ptr<MergePlan>>
OptimizeMergeStrategy::DoCreateMergePlan(const framework::IndexTaskContext* context,
                                         const vector<std::shared_ptr<framework::Segment>>& srcSegments)
{
    auto tabletData = context->GetTabletData();
    auto mergePlan = std::make_shared<MergePlan>(MERGE_PLAN, MERGE_PLAN);
    if (_params.afterMergeMaxSegmentCount != OptimizeMergeParams::INVALID_AFTER_MERGE_MAX_SEGMENT_COUNT) {
        auto result = SplitDocCount(srcSegments);
        if (!result.first.IsOK()) {
            return std::make_pair(result.first, nullptr);
        }
        _params.afterMergeMaxDocCount = result.second;
    }
    size_t segmentIdx = 0;
    while (segmentIdx < srcSegments.size()) {
        SegmentMergePlan plan;
        uint64_t docCount = 0;
        do {
            auto result = NormalTableMergeStrategyUtil::GetDeleteDocCount(srcSegments[segmentIdx].get());
            if (!result.first.IsOK()) {
                return std::make_pair(result.first, nullptr);
            }
            uint32_t acutalSegmentDocCount = srcSegments[segmentIdx]->GetSegmentInfo()->docCount - result.second;
            plan.AddSrcSegment(srcSegments[segmentIdx]->GetSegmentId());
            docCount += acutalSegmentDocCount;
            segmentIdx++;
        } while (segmentIdx < srcSegments.size() && docCount < _params.afterMergeMaxDocCount);
        mergePlan->AddMergePlan(plan);
    }

    if (!NeedMerge(context->GetTabletData(), mergePlan)) {
        AUTIL_LOG(WARN, "no need merge, return empty merge plan");
        return std::make_pair(Status::OK(), std::make_shared<MergePlan>(MERGE_PLAN, MERGE_PLAN));
    }
    RETURN2_IF_STATUS_ERROR(MergeStrategy::FillMergePlanTargetInfo(context, mergePlan), mergePlan,
                            "fill merge plan target info failed");
    AUTIL_LOG(INFO, "merge strategy finish collect segment merge plans");
    return std::make_pair(Status::OK(), mergePlan);
}

vector<std::shared_ptr<framework::Segment>>
OptimizeMergeStrategy::CollectSrcSegments(const std::shared_ptr<framework::TabletData>& tabletData, bool optimize)
{
    vector<std::shared_ptr<framework::Segment>> segments;
    auto slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILT);
    for (auto iter = slice.begin(); iter != slice.end(); iter++) {
        auto segment = *iter;
        if (optimize || _params.maxDocCount == OptimizeMergeParams::DEFAULT_MAX_DOC_COUNT) {
            segments.push_back(segment);
            continue;
        }
        if (segment->GetSegmentInfo()->docCount <= _params.maxDocCount) {
            segments.push_back(segment);
            continue;
        }
    }
    return segments;
}

bool OptimizeMergeStrategy::NeedMerge(const std::shared_ptr<framework::TabletData>& tabletData,
                                      const std::shared_ptr<MergePlan>& plans) const
{
    for (size_t i = 0; i < plans->Size(); ++i) {
        const auto& plan = plans->GetSegmentMergePlan(i);
        if (plan.GetSrcSegmentCount() > 1) {
            return true;
        }
        assert(plan.GetSrcSegmentCount() == 1);
        auto segmentId = plan.GetSrcSegmentId(0);
        auto segment = tabletData->GetSegment(segmentId);
        if (!_params.skipSingleMergedSegment || !segment->GetSegmentInfo()->mergedSegment) {
            return true;
        }
    }
    return false;
}

}} // namespace indexlibv2::table
