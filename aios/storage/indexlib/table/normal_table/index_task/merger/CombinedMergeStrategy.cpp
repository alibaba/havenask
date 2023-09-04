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
#include "indexlib/table/normal_table/index_task/merger/CombinedMergeStrategy.h"

#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/index_task/merger/BalanceTreeMergeStrategy.h"
#include "indexlib/table/normal_table/index_task/merger/PriorityQueueMergeStrategy.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, CombinedMergeStrategy);

std::pair<Status, std::shared_ptr<MergePlan>>
CombinedMergeStrategy::GetMergedSegmentMergePlan(const framework::IndexTaskContext* context) const
{
    std::shared_ptr<MergePlan> mergePlan;
    Status st = Status::OK();
    auto strategyName = _mergedSegmentStrategy->GetName();
    if (strategyName == MergeStrategyDefine::BALANCE_TREE_MERGE_STRATEGY_NAME) {
        auto strategy = dynamic_cast<BalanceTreeMergeStrategy*>(_mergedSegmentStrategy.get());
        assert(strategy != nullptr);
        std::tie(st, mergePlan) = strategy->DoCreateMergePlan(context);
    } else if (strategyName == MergeStrategyDefine::PRIORITY_QUEUE_MERGE_STRATEGY_NAME) {
        auto strategy = dynamic_cast<PriorityQueueMergeStrategy*>(_mergedSegmentStrategy.get());
        assert(strategy != nullptr);
        std::tie(st, mergePlan) = strategy->DoCreateMergePlan(context);
    } else {
        AUTIL_LOG(ERROR, "un-supported merge strategy [%s] for merged segment", strategyName.c_str());
        assert(false);
        return {Status::Corruption(), nullptr};
    }
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "get merged segment merge plan failed, merge strategy[%s]", strategyName.c_str());
        return {st, nullptr};
    }
    AUTIL_LOG(INFO, "get merged segment merge plan success, merge strategy[%s]", strategyName.c_str());
    return {Status::OK(), mergePlan};
}

std::pair<Status, std::shared_ptr<MergePlan>>
CombinedMergeStrategy::CreateMergePlan(const framework::IndexTaskContext* context)
{
    auto [st1, mergedSegmentsMergePlan] = GetMergedSegmentMergePlan(context);
    RETURN2_IF_STATUS_ERROR(st1, nullptr, "get merged segment merge plan failed");

    _buildSegmentStrategy = std::make_unique<RealtimeMergeStrategy>();
    auto mergeConfig = context->GetMergeConfig();
    auto [st2, buildSegmentsMergePlan] =
        _buildSegmentStrategy->DoCreateMergePlan(context, mergeConfig.GetMergeStrategyParameter());
    RETURN2_IF_STATUS_ERROR(st2, nullptr, "Realtime merge strategy create merge plan failed");

    auto mergePlan = std::make_shared<MergePlan>(MERGE_PLAN, MERGE_PLAN);
    if (mergedSegmentsMergePlan != nullptr) {
        for (int i = 0; i < mergedSegmentsMergePlan->Size(); ++i) {
            mergePlan->AddMergePlan(mergedSegmentsMergePlan->GetSegmentMergePlan(i));
        }
    }
    if (buildSegmentsMergePlan != nullptr) {
        for (int i = 0; i < buildSegmentsMergePlan->Size(); ++i) {
            mergePlan->AddMergePlan(buildSegmentsMergePlan->GetSegmentMergePlan(i));
        }
    }

    segmentid_t lastMergedSegmentId = context->GetMaxMergedSegmentId();
    for (size_t planIdx = 0; planIdx < mergePlan->Size(); ++planIdx) {
        SegmentMergePlan* segmentMergePlan = mergePlan->MutableSegmentMergePlan(planIdx);
        RETURN2_IF_STATUS_ERROR(
            FullFillSegmentMergePlan(context->GetTabletData(), &lastMergedSegmentId, segmentMergePlan), nullptr,
            "update segment merge plan failed");
    }

    framework::Version targetVersion = MergePlan::CreateNewVersion(mergePlan, context);
    if (!targetVersion.IsValid()) {
        AUTIL_LOG(ERROR, "create new version failed");
        return {Status::Corruption("Create new version failed"), nullptr};
    }
    mergePlan->SetTargetVersion(std::move(targetVersion));

    AUTIL_LOG(INFO, "Create merge plan %s", ToJsonString(mergePlan, true).c_str());
    return std::make_pair(Status::OK(), mergePlan);
}

Status CombinedMergeStrategy::FullFillSegmentMergePlan(const std::shared_ptr<framework::TabletData>& tabletData,
                                                       segmentid_t* targetSegmentId, SegmentMergePlan* segmentMergePlan)
{
    framework::SegmentInfo targetSegmentInfo;
    std::string targetGroup;
    targetSegmentInfo.mergedSegment = true;
    for (size_t i = 0; i < segmentMergePlan->GetSrcSegmentCount(); ++i) {
        segmentid_t srcSegmentId = segmentMergePlan->GetSrcSegmentId(i);
        SegmentMergePlan::UpdateTargetSegmentInfo(srcSegmentId, tabletData, &targetSegmentInfo);
        auto srcSegment = tabletData->GetSegment(srcSegmentId);
        std::string srcGroupName;
        auto [status, segmentStats] = srcSegment->GetSegmentInfo()->GetSegmentStatistics();
        RETURN_IF_STATUS_ERROR(status, "get segment statistics failed");
        [[maybe_unused]] bool hasGroup = segmentStats.GetStatistic(NORMAL_TABLE_GROUP_TAG_KEY, srcGroupName);
        if (i == 0) {
            targetGroup = srcGroupName;
            continue;
        }
        if (targetGroup != srcGroupName) {
            targetGroup.clear();
        }
    }
    (*targetSegmentId)++;
    if (!targetGroup.empty()) {
        framework::SegmentStatistics stat;
        stat.AddStatistic(NORMAL_TABLE_GROUP_TAG_KEY, targetGroup);
        stat.SetSegmentId(*targetSegmentId);
        targetSegmentInfo.SetSegmentStatistics(stat);
    }
    segmentMergePlan->AddTargetSegment(*targetSegmentId, targetSegmentInfo);
    return Status::OK();
}

}} // namespace indexlibv2::table
