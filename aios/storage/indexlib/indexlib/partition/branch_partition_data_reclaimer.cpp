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
#include "indexlib/partition/branch_partition_data_reclaimer.h"

#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/open_executor/open_executor_util.h"

using namespace std;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, BranchPartitionDataReclaimer);

BranchPartitionDataReclaimer::BranchPartitionDataReclaimer(const config::IndexPartitionSchemaPtr& schema,
                                                           const config::IndexPartitionOptions& options)
    : RealtimePartitionDataReclaimerBase(schema)
    , mOptions(options)
{
}

BranchPartitionDataReclaimer::~BranchPartitionDataReclaimer() {}

void BranchPartitionDataReclaimer::DoFinishTrimBuildingSegment(int64_t reclaimTimestamp, int64_t buildingTs,
                                                               const index_base::PartitionDataPtr& partData)
{
}

void BranchPartitionDataReclaimer::DoFinishRemoveObsoleteRtDocs(PartitionModifierPtr modifier,
                                                                const index_base::PartitionDataPtr& partData)
{
}

void BranchPartitionDataReclaimer::DoFinishTrimObsoleteAndEmptyRtSegments(
    const index_base::PartitionDataPtr& partData, const std::vector<segmentid_t>& segIdsToRemove)
{
    if (segIdsToRemove.empty()) {
        return;
    }
    partData->RemoveSegments(segIdsToRemove);
}

PartitionModifierPtr BranchPartitionDataReclaimer::CreateModifier(const index_base::PartitionDataPtr& partData)
{
    return OpenExecutorUtil::CreateInplaceModifier(mOptions, mSchema, partData);
}
}} // namespace indexlib::partition
