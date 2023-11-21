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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/partition/realtime_partition_data_reclaimer_base.h"

namespace indexlib { namespace partition {

class BranchPartitionDataReclaimer : public RealtimePartitionDataReclaimerBase
{
public:
    BranchPartitionDataReclaimer(const config::IndexPartitionSchemaPtr& schema,
                                 const config::IndexPartitionOptions& options);
    ~BranchPartitionDataReclaimer();

protected:
    void DoFinishTrimBuildingSegment(int64_t reclaimTimestamp, int64_t buildingTs,
                                     const index_base::PartitionDataPtr& partData) override;
    void DoFinishRemoveObsoleteRtDocs(PartitionModifierPtr modifier,
                                      const index_base::PartitionDataPtr& partData) override;
    void DoFinishTrimObsoleteAndEmptyRtSegments(const index_base::PartitionDataPtr& partData,
                                                const std::vector<segmentid_t>& segIdsToRemove) override;
    PartitionModifierPtr CreateModifier(const index_base::PartitionDataPtr& partData) override;

private:
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BranchPartitionDataReclaimer);
}} // namespace indexlib::partition
