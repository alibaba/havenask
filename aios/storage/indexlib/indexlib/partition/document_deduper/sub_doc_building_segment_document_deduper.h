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
#include "indexlib/indexlib.h"
#include "indexlib/partition/document_deduper/building_segment_document_deduper.h"
#include "indexlib/partition/document_deduper/document_deduper.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"

namespace indexlib { namespace partition {

class SubDocBuildingSegmentDocumentDeduper : public DocumentDeduper
{
public:
    SubDocBuildingSegmentDocumentDeduper(const config::IndexPartitionSchemaPtr& schema) : DocumentDeduper(schema) {}
    ~SubDocBuildingSegmentDocumentDeduper() {}

public:
    void Init(const index_base::InMemorySegmentPtr& buildingSegment, const partition::PartitionModifierPtr& modifier)
    {
        partition::SubDocModifierPtr subDocModifier = DYNAMIC_POINTER_CAST(partition::SubDocModifier, modifier);
        assert(subDocModifier);

        mMainDeduper.reset(new BuildingSegmentDocumentDeduper(mSchema));
        // subDocModifier get pkReader is mainModifier pkReader
        // subDocModifier remove mainDupPk, can also remove its subDupPk
        mMainDeduper->Init(buildingSegment, subDocModifier);

        mSubDeduper.reset(new BuildingSegmentDocumentDeduper(mSchema->GetSubIndexPartitionSchema()));
        mSubDeduper->Init(buildingSegment->GetSubInMemorySegment(), subDocModifier->GetSubModifier());
    }
    void Dedup() override;

private:
    BuildingSegmentDocumentDeduperPtr mMainDeduper;
    BuildingSegmentDocumentDeduperPtr mSubDeduper;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocBuildingSegmentDocumentDeduper);
///////////////////////////////////////////////////////////////
void SubDocBuildingSegmentDocumentDeduper::Dedup()
{
    mMainDeduper->Dedup();
    mSubDeduper->Dedup();
}
}} // namespace indexlib::partition
