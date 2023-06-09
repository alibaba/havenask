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
#include "indexlib/partition/realtime_partition_data_reclaimer.h"

#include "autil/StringUtil.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/calculator/partition_size_calculator.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, RealtimePartitionDataReclaimer);

RealtimePartitionDataReclaimer::RealtimePartitionDataReclaimer(const IndexPartitionSchemaPtr& schema,
                                                               const config::IndexPartitionOptions& options)
    : RealtimePartitionDataReclaimerBase(schema)
    , mOptions(options)
{
}

RealtimePartitionDataReclaimer::~RealtimePartitionDataReclaimer() {}

void RealtimePartitionDataReclaimer::DoFinishTrimBuildingSegment(int64_t reclaimTimestamp, int64_t buildingTs,
                                                                 const PartitionDataPtr& partData)
{
    if ((buildingTs == INVALID_TIMESTAMP) || (buildingTs < reclaimTimestamp)) {
        const SegmentDirectoryPtr& segDir = partData->GetSegmentDirectory();
        segDir->IncLastSegmentId();
        partData->CommitVersion();
        partData->ResetInMemorySegment();
        IE_LOG(INFO, "building segment droped");
    }

    IE_LOG(INFO, "trim building segment end");
}

void RealtimePartitionDataReclaimer::DoFinishRemoveObsoleteRtDocs(PartitionModifierPtr modifier,
                                                                  const PartitionDataPtr& partitionData)
{
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    NormalSegmentDumpItemPtr segmentDumpItem(new NormalSegmentDumpItem(mOptions, mSchema, ""));
    segmentDumpItem->Init(NULL, partitionData, modifier->GetDumpTaskItem(modifier), FlushedLocatorContainerPtr(), true);
    segmentDumpItem->Dump();
    partitionData->AddBuiltSegment(inMemSegment->GetSegmentId(), inMemSegment->GetSegmentInfo());
    partitionData->CommitVersion();
    inMemSegment->SetStatus(InMemorySegment::DUMPED);
    partitionData->CreateNewSegment();
}

PartitionModifierPtr RealtimePartitionDataReclaimer::CreateModifier(const index_base::PartitionDataPtr& partData)
{
    PartitionModifierPtr modifier(PartitionModifierCreator::CreatePatchModifier(mSchema, partData, false, false));
    return modifier;
}

void RealtimePartitionDataReclaimer::DoFinishTrimObsoleteAndEmptyRtSegments(const PartitionDataPtr& partData,
                                                                            const vector<segmentid_t>& segIdsToRemove)
{
    if (segIdsToRemove.empty()) {
        return;
    }
    partData->RemoveSegments(segIdsToRemove);
    partData->CommitVersion();
    partData->CreateNewSegment();
}
}} // namespace indexlib::partition
