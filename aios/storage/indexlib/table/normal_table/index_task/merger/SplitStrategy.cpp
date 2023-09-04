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
#include "indexlib/table/normal_table/index_task/merger/SplitStrategy.h"

#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/config/SegmentGroupConfig.h"
#include "indexlib/table/normal_table/index_task/SingleSegmentDocumentGroupSelector.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, SplitStrategy);

SplitStrategy::SplitStrategy() {}

SplitStrategy::~SplitStrategy() {}

std::pair<Status, std::vector<int32_t>> SplitStrategy::CalculateGroupCounts(const framework::IndexTaskContext* context,
                                                                            std::shared_ptr<framework::Segment> segment)
{
    auto docCount = segment->GetSegmentInfo()->docCount;
    std::vector<int32_t> counts(_segmentGroupConfig->GetGroups().size(), 0);
    auto groupSelector = std::make_shared<SingleSegmentDocumentGroupSelector>();
    RETURN2_IF_STATUS_ERROR(groupSelector->Init(segment, context->GetTabletSchema()), std::vector<int32_t>(),
                            "init group selector failed");
    for (docid_t docid = 0; docid < docCount; ++docid) {
        auto [status, groupId] = groupSelector->Select(docid);
        RETURN2_IF_STATUS_ERROR(status, std::vector<int32_t>(), "calculate groupd id for docid [%d] failed", docid);
        assert(groupId < counts.size());
        counts[groupId]++;
    }
    RETURN2_IF_STATUS_ERROR(groupSelector->Dump(context->GetResourceManager()), counts,
                            "dump group seletor resource failed");
    return {Status::OK(), counts};
}

Status SplitStrategy::CalculateSegmentStatistics(const framework::IndexTaskContext* context,
                                                 std::vector<std::pair<double, segmentid_t>>* segmentPriority,
                                                 std::map<segmentid_t, std::vector<int32_t>>* segmentGroupDocCounts)
{
    auto tabletData = context->GetTabletData();
    for (auto segment : tabletData->CreateSlice()) {
        auto segmentId = segment->GetSegmentId();
        auto docCount = segment->GetSegmentInfo()->docCount;
        if (docCount == 0) {
            continue;
        }
        if (!framework::Segment::IsMergedSegmentId(segmentId)) {
            AUTIL_LOG(INFO, "ignore segment [%d], not merged segment", segmentId);
        } else {
            std::string currentSegmentGroup;
            double changeRatio = 1;
            auto [status, counts] = CalculateGroupCounts(context, segment);
            RETURN_IF_STATUS_ERROR(status, "calculate group counts failed");
            auto [status2, segmentStats] = segment->GetSegmentInfo()->GetSegmentStatistics();
            RETURN_IF_STATUS_ERROR(status2, "get segment statistics failed");
            if (segmentStats.GetStatistic(NORMAL_TABLE_GROUP_TAG_KEY, currentSegmentGroup)) {
                for (size_t i = 0; i < _segmentGroupConfig->GetGroups().size(); ++i) {
                    if (_segmentGroupConfig->GetGroups()[i] == currentSegmentGroup) {
                        changeRatio = 1.0 - (counts[i] * 1.0) / docCount;
                        break;
                    }
                }
            }
            if (changeRatio != 0) {
                segmentPriority->emplace_back(changeRatio, segmentId);
                (*segmentGroupDocCounts)[segmentId] = counts;
            }
        }
    }
    std::sort(segmentPriority->begin(), segmentPriority->end());
    AUTIL_LOG(INFO, "segment priority [%s]", autil::legacy::ToJsonString(*segmentPriority, true).c_str());
    return Status::OK();
}

std::pair<Status, std::shared_ptr<MergePlan>> SplitStrategy::CreateMergePlan(const framework::IndexTaskContext* context)
{
    RETURN2_IF_STATUS_ERROR(InitFromContext(context), nullptr, "init from context failed");

    std::vector<std::pair<double, segmentid_t>> segmentPriority;
    std::map<segmentid_t, std::vector<int32_t>> segmentGroupDocCounts;
    RETURN2_IF_STATUS_ERROR(CalculateSegmentStatistics(context, &segmentPriority, &segmentGroupDocCounts), nullptr,
                            "caculate segment statistics failed");
    auto mergePlan = std::make_shared<MergePlan>(/*name=*/MERGE_PLAN, /*type=*/MERGE_PLAN);
    auto totalMergedDocCount = 0;
    auto tabletData = context->GetTabletData();
    segmentid_t lastMergedSegmentId = context->GetMaxMergedSegmentId();
    for (auto [_, segmentId] : segmentPriority) {
        auto segmentDocCount = tabletData->GetSegment(segmentId)->GetSegmentInfo()->docCount;
        if (totalMergedDocCount + segmentDocCount <= _outputLimits.maxTotalMergedDocCount) {
            SegmentMergePlan segmentMergePlan;
            segmentMergePlan.AddSrcSegment(segmentId);
            auto groups = _segmentGroupConfig->GetGroups();
            for (size_t groupId = 0; groupId < groups.size(); ++groupId) {
                int32_t targetSegmentDocCount = segmentGroupDocCounts[segmentId][groupId];
                // 最后一个default group是空跳过，否则其他group没有数据dump空segment
                if (targetSegmentDocCount == 0 && groupId + 1 == groups.size()) {
                    continue;
                }
                const auto& groupName = groups[groupId];
                framework::SegmentInfo targetSegmentInfo;
                framework::SegmentStatistics segmentStatistics;
                segmentid_t targetSegmentId = ++lastMergedSegmentId;
                segmentStatistics.SetSegmentId(targetSegmentId);
                segmentStatistics.AddStatistic(NORMAL_TABLE_GROUP_TAG_KEY, groupName);
                targetSegmentInfo.SetSegmentStatistics(segmentStatistics);
                SegmentMergePlan::UpdateTargetSegmentInfo(segmentId, tabletData, &targetSegmentInfo);
                targetSegmentInfo.mergedSegment = true;
                targetSegmentInfo.docCount = targetSegmentDocCount;
                segmentMergePlan.AddTargetSegment(targetSegmentId, targetSegmentInfo);
            }
            mergePlan->AddMergePlan(segmentMergePlan);
            totalMergedDocCount += segmentDocCount;
        }
    }
    framework::Version targetVersion = MergePlan::CreateNewVersion(mergePlan, context);
    if (!targetVersion.IsValid()) {
        AUTIL_LOG(ERROR, "create new version failed");
        return {Status::Corruption("Create new version failed"), nullptr};
    }
    mergePlan->SetTargetVersion(std::move(targetVersion));
    AUTIL_LOG(INFO, "create split plan [%s] success", autil::legacy::ToJsonString(mergePlan, true).c_str());
    return {Status::OK(), mergePlan};
}

Status SplitStrategy::InitFromContext(const framework::IndexTaskContext* context)
{
    if (!context) {
        AUTIL_LOG(ERROR, "context is empty");
        return Status::InvalidArgs("context is empty");
    }
    if (!context->GetTabletData()) {
        AUTIL_LOG(ERROR, "tablet data is empty");
        return Status::InvalidArgs("tablet data is empty");
    }
    auto schema = context->GetTabletSchema();
    if (!schema) {
        AUTIL_LOG(ERROR, "schema is empty");
        return Status::InvalidArgs("hema is empty");
    }
    auto [status, segmentGroupConfig] =
        schema->GetRuntimeSettings().GetValue<std::shared_ptr<SegmentGroupConfig>>(NORMAL_TABLE_GROUP_CONFIG_KEY);
    RETURN_IF_STATUS_ERROR(status, "get segment group config failed");
    assert(segmentGroupConfig);
    if (!segmentGroupConfig->IsGroupEnabled()) {
        AUTIL_LOG(ERROR, "segment groups is empty");
        return Status::InvalidArgs("segment groups is empty");
    }
    _segmentGroupConfig = segmentGroupConfig;
    AUTIL_LOG(INFO, "init from context finished, segment group config:[%s]",
              autil::legacy::ToJsonString(*_segmentGroupConfig, true).c_str());
    auto mergeConfig = context->GetMergeConfig();
    auto outputLimitStr = mergeConfig.GetMergeStrategyParameter()._impl->outputLimitParam;
    if (!_outputLimits.FromString(outputLimitStr)) {
        AUTIL_LOG(ERROR, "init output limit from [%s] failed", outputLimitStr.c_str());
        return Status::InvalidArgs("output limit str invalie");
    }
    return Status::OK();
}

} // namespace indexlibv2::table
