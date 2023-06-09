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
#include "indexlib/partition/modifier/partition_modifier.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_iterator.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionModifier);

PartitionModifier::PartitionModifier(const IndexPartitionSchemaPtr& schema)
    : mSchema(schema)
    , mSupportAutoUpdate(false)
    , mDumpThreadNum(1)
{
    if (mSchema) {
        mSupportAutoUpdate = mSchema->SupportAutoUpdate();
    }
}

int64_t PartitionModifier::GetTotalMemoryUse() const
{
    assert(mBuildResourceMetrics);
    return mBuildResourceMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE);
}

bool PartitionModifier::UpdateField(const index::AttrFieldValue& value)
{
    if (value.IsPackAttr()) {
        return UpdatePackField(value.GetDocId(), value.GetPackAttrId(), *value.GetConstStringData());
    }

    return UpdateField(value.GetDocId(), value.GetFieldId(), *value.GetConstStringData(), value.IsNull());
}

const util::BuildResourceMetricsPtr& PartitionModifier::GetBuildResourceMetrics() const
{
    return mBuildResourceMetrics;
}

docid_t PartitionModifier::CalculateBuildingSegmentBaseDocId(const index_base::PartitionDataPtr& partitionData)
{
    assert(partitionData);
    index_base::PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);

    docid_t baseDocid = segIter->GetBuildingBaseDocId();
    index_base::SegmentIteratorPtr buildingIter = segIter->CreateIterator(index_base::SIT_BUILDING);
    while (buildingIter->IsValid()) {
        index_base::InMemorySegmentPtr inMemSegment = buildingIter->GetInMemSegment();
        assert(inMemSegment);
        if (inMemSegment->GetStatus() != index_base::InMemorySegment::BUILDING) {
            baseDocid += buildingIter->GetSegmentData().GetSegmentInfo()->docCount;
        }
        buildingIter->MoveToNext();
    }
    return baseDocid;
}

}} // namespace indexlib::partition
