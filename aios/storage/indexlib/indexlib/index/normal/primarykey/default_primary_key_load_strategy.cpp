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
#include "indexlib/index/normal/primarykey/default_primary_key_load_strategy.h"

#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DefaultPrimaryKeyLoadStrategy);

DefaultPrimaryKeyLoadStrategy::~DefaultPrimaryKeyLoadStrategy() {}

void DefaultPrimaryKeyLoadStrategy::CreatePrimaryKeyLoadPlans(const index_base::PartitionDataPtr& partitionData,
                                                              PrimaryKeyLoadPlanVector& plans)
{
    docid_t baseDocid = 0;
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
        PrimaryKeyLoadPlanPtr plan(new PrimaryKeyLoadPlan());
        plan->Init(baseDocid, mPkConfig);
        plan->AddSegmentData(segData);
        baseDocid += docCount;
        plans.push_back(plan);
        builtSegIter->MoveToNext();
    }
}
}} // namespace indexlib::index
