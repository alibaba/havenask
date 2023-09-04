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
#include "indexlib/table/normal_table/index_task/merger/AdaptiveMergeStrategy.h"

#include <cmath>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/normal_table/index_task/merger/NormalTableMergeStrategyUtil.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, AdaptiveMergeStrategy);

AdaptiveMergeStrategy::AdaptiveMergeStrategy() : _realtimeMergeStrategy(std::make_unique<RealtimeMergeStrategy>()) {}

AdaptiveMergeStrategy::~AdaptiveMergeStrategy() {}

size_t AdaptiveMergeStrategy::GetValidSegmentSize(size_t docCount, size_t deleteDocCount, size_t segmentSize)
{
    if (docCount > 0) {
        return segmentSize * ((static_cast<double>(docCount - deleteDocCount)) / docCount);
    }
    return 0;
}

std::pair<Status, std::shared_ptr<MergePlan>>
AdaptiveMergeStrategy::CreateMergePlan(const framework::IndexTaskContext* context)
{
    auto [status, mergePlan] = DoCreateMergePlan(context);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "Create merge plan failed");
    RETURN2_IF_STATUS_ERROR(MergeStrategy::FillMergePlanTargetInfo(context, mergePlan), mergePlan,
                            "fill merge plan target info failed");
    AUTIL_LOG(INFO, "Create merge plan %s", ToJsonString(mergePlan, true).c_str());
    return {Status::OK(), mergePlan};
}

std::vector<segmentid_t>
AdaptiveMergeStrategy::FigureOutFinalCandidates(std::vector<std::tuple<segmentid_t, size_t, size_t>> estimateValues)
{
    std::vector<segmentid_t> finalCandidates;
    if (estimateValues.size() > DYNAMIC_PROGRAMMING_SEGMENT_LIMIT) {
        // 01背包的近似算法，单位价值最大优先填充。
        std::sort(estimateValues.begin(), estimateValues.end(), [](const auto& lhs, const auto& rhs) {
            const auto& [_, lCost, lValue] = lhs;
            const auto& [__, rCost, rValue] = rhs;
            return lValue * rCost > rValue * lCost;
        });

        size_t totalCost = 0;
        for (auto& [segmentId, cost, value] : estimateValues) {
            if (cost + totalCost > DYNAMIC_PROGRAMMING_NORMALIZED_COST) {
                continue;
            }
            finalCandidates.push_back(segmentId);
            totalCost += cost;
        }
    } else {
        // 01背包的精确算法
        std::pair<size_t /*value*/, uint64_t /*candidates*/> dp[DYNAMIC_PROGRAMMING_NORMALIZED_COST + 1];
        for (size_t i = 0; i <= DYNAMIC_PROGRAMMING_NORMALIZED_COST; ++i) {
            dp[i] = std::make_pair<size_t, uint64_t>(0, 0);
        }

        for (size_t idx = 0; idx < estimateValues.size(); ++idx) {
            auto [_, cost, value] = estimateValues[idx];
            for (size_t costTotal = DYNAMIC_PROGRAMMING_NORMALIZED_COST; costTotal >= cost; --costTotal) {
                auto [val, candidates] = dp[costTotal - cost];
                auto newItem = std::make_pair<size_t, uint64_t>(val + value, candidates | (1lu << idx));
                dp[costTotal] = std::max(dp[costTotal], newItem);
            }
        }

        uint64_t mask = dp[DYNAMIC_PROGRAMMING_NORMALIZED_COST].second;
        for (size_t idx = 0; idx < estimateValues.size(); ++idx) {
            if (mask & (1lu << idx)) {
                auto [segmentId, _, __] = estimateValues[idx];
                finalCandidates.push_back(segmentId);
            }
        }
    }
    return finalCandidates;
}

std::pair<Status, std::shared_ptr<MergePlan>>
AdaptiveMergeStrategy::DoCreateMergePlan(const framework::IndexTaskContext* context)
{
    auto mergeConfig = context->GetMergeConfig();
    assert(mergeConfig.GetMergeStrategyStr() == GetName());

    size_t totalSize = 0;
    size_t totalDocCount = 0;
    size_t totalDeletedDocCount = 0;
    std::map<segmentid_t, size_t> validSegmentSizeMap;
    std::map<segmentid_t, size_t> deletedDocCountSizeMap;
    std::vector<std::shared_ptr<framework::Segment>> mergedSegments;

    for (const auto& segment : context->GetTabletData()->CreateSlice()) {
        auto [status, segmentSize] = segment->GetSegmentDirectory()->GetIDirectory()->GetDirectorySize("").StatusWith();
        RETURN2_IF_STATUS_ERROR(status, nullptr, "get directory size fail, dir[%s]",
                                segment->GetSegmentDirectory()->GetLogicalPath().c_str());
        auto validSegmentSize = segmentSize;
        size_t docCount = segment->GetSegmentInfo()->docCount;
        totalDocCount += docCount;
        if (segment->GetSegmentInfo()->IsMergedSegment()) {
            mergedSegments.push_back(segment);
            auto [status, deleteDocCount] = NormalTableMergeStrategyUtil::GetDeleteDocCount(segment.get());
            assert(status.IsOK());
            validSegmentSize = GetValidSegmentSize(docCount, deleteDocCount, segmentSize);
            deletedDocCountSizeMap[segment->GetSegmentId()] = deleteDocCount;
            totalDeletedDocCount += deleteDocCount;
        }
        validSegmentSizeMap[segment->GetSegmentId()] = validSegmentSize;
        totalSize += validSegmentSize;
    }

    // 2^21 ->10, 2^31 -> 40
    double idealSegmentCount = totalDocCount < DOC_BASE_LINE ? 10.0 : (10.0 + 3 * (std::log2(totalDocCount) - 21.0));
    assert(idealSegmentCount >= 10.0 && idealSegmentCount <= 40.0);

    size_t idealQuota = 1.0 * totalSize / idealSegmentCount * QUOTA_AS_IDEAL_SEGMEN_COUNT;
    assert(totalDocCount <= DOC_DEAD_LINE);

    double avgDeletedDocRatio = 1.0 * totalDeletedDocCount / totalDocCount;
    double emergencyArgForDeadLine = 1.0 * EMERGENCY_SPACE / (DOC_DEAD_LINE - totalDocCount);
    double emergencyArgForDeletedDoc = avgDeletedDocRatio / EMERGENCY_DELETED_DOC_RATIO;
    emergencyArgForDeletedDoc = std::min(emergencyArgForDeletedDoc, std::sqrt(emergencyArgForDeletedDoc));
    double emergencyArg = std::min(7.0, (emergencyArgForDeadLine + 1) * emergencyArgForDeletedDoc);
    size_t emergencyQuota = idealQuota * emergencyArg;
    size_t realtimeQuota = std::max(idealQuota, 2048lu * 1024 * 1024 /*2GB*/);

    auto param = indexlibv2::config::MergeStrategyParameter::Create(
        "", "", "rt-max-total-merge-size=" + std::to_string(realtimeQuota / 1024 / 1024));

    auto [status, mergePlan] = _realtimeMergeStrategy->DoCreateMergePlan(context, param);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "realtime strategy create merge plan failed with param [%s]",
                            param.GetLegacyString().c_str());

    assert(mergePlan->Size() <= 1);
    size_t realtimeUsedQuota = 0;
    for (size_t i = 0; i < mergePlan->Size(); ++i) {
        auto segmentMergePlan = mergePlan->GetSegmentMergePlan(i);
        for (auto segmentId : segmentMergePlan.GetSrcSegments()) {
            realtimeUsedQuota += validSegmentSizeMap[segmentId];
        }
    }

    if (realtimeUsedQuota >= idealQuota + emergencyQuota) {
        return {Status::OK(), mergePlan};
    }
    size_t quota = idealQuota - realtimeUsedQuota + emergencyQuota;

    AUTIL_LOG(INFO,
              "realtime segment use quota [%zu], left quota [%zu], emergency quota [%zu], standard quota [%zu], "
              "avgDeletedDocRatio [%lf]",
              realtimeUsedQuota, quota, emergencyQuota, idealQuota, avgDeletedDocRatio);

    std::vector<std::tuple<segmentid_t, size_t, size_t>> estimateValues;
    for (const auto& segment : mergedSegments) {
        auto segmentId = segment->GetSegmentId();
        size_t validSegmentSize = validSegmentSizeMap[segmentId];
        size_t deleteDocCount = deletedDocCountSizeMap[segmentId];
        size_t segmentDocCount = segment->GetSegmentInfo()->GetDocCount();
        if (validSegmentSize > quota) {
            // 超过整个quota的不考虑
            continue;
        }
        if ((1.0 * deleteDocCount / segmentDocCount) < 0.5 * avgDeletedDocRatio && validSegmentSize >= idealQuota) {
            // deleted doc占比不及平均，并且大小还比标准quota大，收益小不考虑
            continue;
        }
        size_t normalizedCost = DYNAMIC_PROGRAMMING_NORMALIZED_COST * validSegmentSize / quota + 1;
        size_t estimateValue = emergencyArg * deleteDocCount + mergedSegments.size();
        estimateValues.push_back({segmentId, normalizedCost, estimateValue});
    }

    auto finalCandidates = FigureOutFinalCandidates(estimateValues);
    size_t totalQuotaUsed = realtimeUsedQuota;
    for (auto& segmentId : finalCandidates) {
        size_t validSegmentSize = validSegmentSizeMap[segmentId];
        AUTIL_LOG(INFO, "merge quota used [%zu] for segmentId [%d]", validSegmentSize, segmentId);
        totalQuotaUsed += validSegmentSize;
    }

    size_t quotaUseRatio = 100 * totalQuotaUsed / (idealQuota + emergencyQuota);
    assert(quotaUseRatio <= 100);
    AUTIL_LOG(INFO, "merge total quota used [%zu], ratio [%zu]", totalQuotaUsed, quotaUseRatio);

    for (size_t i = 0; i < finalCandidates.size(); ++i) {
        auto segmentId = finalCandidates[i];
        SegmentMergePlan segmentMergePlan;
        segmentMergePlan.AddSrcSegment(segmentId);
        size_t totalSize = validSegmentSizeMap[segmentId];
        while (i + 1 < finalCandidates.size()) {
            auto segmentId = finalCandidates[i + 1];
            size_t validSegmentSize = validSegmentSizeMap[segmentId];
            if (totalSize + validSegmentSize > idealQuota) {
                break;
            }
            totalSize += validSegmentSize;
            segmentMergePlan.AddSrcSegment(segmentId);
            ++i;
        }
        mergePlan->AddMergePlan(segmentMergePlan);
    }
    return {Status::OK(), mergePlan};
}

} // namespace indexlibv2::table
