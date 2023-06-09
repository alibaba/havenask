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
#include "indexlib/table/index_task/merger/MergeStrategy.h"

#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/SegmentMergePlan.h"

using namespace std;

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, MergeStrategy);

Status MergeStrategy::FillMergePlanTargetInfo(const framework::IndexTaskContext* context,
                                              std::shared_ptr<MergePlan> mergePlan)
{
    if (unlikely(!mergePlan)) {
        assert(false);
        mergePlan.reset(new MergePlan(MERGE_PLAN, MERGE_PLAN));
    }
    segmentid_t lastMergedSegmentId = context->GetMaxMergedSegmentId();
    for (size_t planIdx = 0; planIdx < mergePlan->Size(); ++planIdx) {
        SegmentMergePlan* segmentMergePlan = mergePlan->MutableSegmentMergePlan(planIdx);

        framework::SegmentInfo targetSegmentInfo;
        targetSegmentInfo.mergedSegment = true;
        for (size_t i = 0; i < segmentMergePlan->GetSrcSegmentCount(); ++i) {
            segmentid_t srcSegmentId = segmentMergePlan->GetSrcSegmentId(i);
            SegmentMergePlan::UpdateTargetSegmentInfo(srcSegmentId, context->GetTabletData(), &targetSegmentInfo);
        }
        segmentid_t targetSegmentId = ++lastMergedSegmentId;
        segmentMergePlan->AddTargetSegment(targetSegmentId, targetSegmentInfo);
    }

    framework::Version targetVersion = MergePlan::CreateNewVersion(mergePlan, context);
    if (!targetVersion.IsValid()) {
        AUTIL_LOG(ERROR, "create new version failed");
        return Status::Corruption("Create new version failed");
    }
    mergePlan->SetTargetVersion(std::move(targetVersion));
    return Status::OK();
}

}} // namespace indexlibv2::table
