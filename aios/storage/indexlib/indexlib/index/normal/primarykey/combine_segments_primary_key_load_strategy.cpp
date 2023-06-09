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
#include "indexlib/index/normal/primarykey/combine_segments_primary_key_load_strategy.h"

#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, CombineSegmentsPrimaryKeyLoadStrategy);

void CombineSegmentsPrimaryKeyLoadStrategy::CreatePrimaryKeyLoadPlans(const index_base::PartitionDataPtr& partitionData,
                                                                      PrimaryKeyLoadPlanVector& plans)
{
    PrimaryKeyLoadPlanPtr plan;
    docid_t baseDocid = 0;
    bool isRtPlan = false;

    PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    SegmentIteratorPtr builtSegIter = segIter->CreateIterator(SIT_BUILT);
    while (builtSegIter->IsValid()) {
        SegmentData segData = builtSegIter->GetSegmentData();
        size_t docCount = segData.GetSegmentInfo()->docCount;
        if (docCount == 0) {
            builtSegIter->MoveToNext();
            continue;
        }
        bool isRtSegment = RealtimeSegmentDirectory::IsRtSegmentId(segData.GetSegmentId());
        if (NeedCreateNewPlan(isRtSegment, plan, isRtPlan)) {
            PushBackPlan(plan, plans);
            plan.reset(new PrimaryKeyLoadPlan());
            plan->Init(baseDocid, mPkConfig);
            isRtPlan = isRtSegment;
        }
        plan->AddSegmentData(segData);
        baseDocid += docCount;
        builtSegIter->MoveToNext();
    }
    PushBackPlan(plan, plans);
}

void CombineSegmentsPrimaryKeyLoadStrategy::PushBackPlan(const PrimaryKeyLoadPlanPtr& plan,
                                                         PrimaryKeyLoadPlanVector& plans)
{
    if (plan && plan->GetDocCount() > 0) {
        plans.push_back(plan);
    }
}

bool CombineSegmentsPrimaryKeyLoadStrategy::NeedCreateNewPlan(bool isRtSegment, const PrimaryKeyLoadPlanPtr& plan,
                                                              bool isRtPlan)
{
    if (!plan) {
        return true;
    }

    if (isRtPlan != isRtSegment) {
        return true;
    }
    // deal with segment doc count > mMaxDocCount
    size_t currentDocCount = plan->GetDocCount();
    if (currentDocCount == 0) {
        return false;
    }

    if (currentDocCount >= mMaxDocCount) {
        return true;
    }
    return false;
}
}} // namespace indexlib::index
