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
#include "indexlib/index/kv/kv_index_options.h"

#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KVIndexOptions);

void KVIndexOptions::Init(const KVIndexConfigPtr& _kvConfig, const PartitionDataPtr& partitionData,
                          int64_t latestNeedSkipIncTs)
{
    kvConfig = _kvConfig;
    lastSkipIncTsInSecond =
        (latestNeedSkipIncTs == INVALID_TIMESTAMP) ? INVALID_TIMESTAMP : MicrosecondToSecond(latestNeedSkipIncTs);
    int64_t tmp = kvConfig->GetTTL();
    ttl = (tmp == INVALID_TTL ? DEFAULT_TIME_TO_LIVE : tmp);
    fixedValueLen = kvConfig->GetValueConfig()->GetFixedLength();
    InitPartitionState(partitionData);
    if (kvConfig && kvConfig->GetRegionCount() > 1) {
        hasMultiRegion = true;
    } else {
        const ValueConfigPtr& valueConfig = kvConfig->GetValueConfig();
        if (!valueConfig->IsSimpleValue()) {
            plainFormatEncoder.reset(
                common::PackAttributeFormatter::CreatePlainFormatEncoder(valueConfig->CreatePackAttributeConfig()));
        }
    }
}

void KVIndexOptions::InitPartitionState(const PartitionDataPtr& partitionData)
{
    assert(partitionData);
    Version version = partitionData->GetOnDiskVersion();
    OnlineJoinPolicy joinPolicy(version, tt_kv, partitionData->GetSrcSignature());
    int64_t incTs = joinPolicy.GetRtSeekTimestampFromIncVersion();

    if (incTs == INVALID_TIMESTAMP) {
        incTs = 0;
    }
    incTsInSecond = MicrosecondToSecond(incTs);
    PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    SegmentIteratorPtr buildingSegIter = segIter->CreateIterator(SIT_BUILDING);
    assert(buildingSegIter);
    assert(buildingSegmentId == INVALID_SEGMENTID);
    // note : only built segment in cache, so make buildingSegmentId equal to first inMemSegment
    buildingSegmentId = buildingSegIter->IsValid() ? buildingSegIter->GetSegmentData().GetSegmentId()
                                                   : segIter->GetNextBuildingSegmentId();
    // hasBuilding = buildingSegIter->IsValid();
    assert(oldestRtSegmentId == INVALID_SEGMENTID);
    while (segIter->IsValid()) {
        segmentid_t segId = segIter->GetSegmentId();
        if (RealtimeSegmentDirectory::IsRtSegmentId(segId)) {
            oldestRtSegmentId = segId;
            // hasOnlineSegment = true;
            break;
        }
        segIter->MoveToNext();
    }
    if (oldestRtSegmentId == INVALID_SEGMENTID) {
        oldestRtSegmentId = buildingSegmentId;
    }
}
}} // namespace indexlib::index
