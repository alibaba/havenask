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
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/modifier/in_memory_segment_modifier.h"
#include "indexlib/partition/modifier/partition_modifier.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, InplaceAttributeModifier);
DECLARE_REFERENCE_CLASS(index, InplaceIndexModifier);
DECLARE_REFERENCE_CLASS(document, ModifiedTokens);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index, AttributeUpdateBitmap);

namespace indexlib { namespace partition {

class InplaceModifier : public PartitionModifier
{
public:
    InplaceModifier(const config::IndexPartitionSchemaPtr& schema);
    ~InplaceModifier();

public:
    void Init(const IndexPartitionReaderPtr& partitionReader, const index_base::InMemorySegmentPtr& inMemSegment,
              const util::BuildResourceMetricsPtr& buildResourceMetrics = util::BuildResourceMetricsPtr());

    bool DedupDocument(const document::DocumentPtr& doc) override { return RemoveDocument(doc); }
    bool UpdateDocument(const document::DocumentPtr& doc) override;
    bool RemoveDocument(const document::DocumentPtr& doc) override;
    void Dump(const file_system::DirectoryPtr& directory, segmentid_t srcSegmentId) override;
    bool IsDirty() const override;
    bool UpdateField(docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull) override;

    bool UpdatePackField(docid_t docId, packattrid_t packAttrId, const autil::StringView& value) override;
    bool RedoIndex(docid_t docId, const document::ModifiedTokens& modifiedTokens) override;
    bool UpdateIndex(index::IndexUpdateTermIterator* iterator) override;

    bool RemoveDocument(docid_t docId) override;
    docid_t GetBuildingSegmentBaseDocId() const override { return mBuildingSegmentBaseDocid; }
    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyIndexReader() const override { return mPkIndexReader; }
    index::PartitionInfoPtr GetPartitionInfo() const override { return mPartitionReader->GetPartitionInfo(); }
    index::DeletionMapReaderPtr GetDeletionMapReader() const { return mDeletionmapReader; }
    index::InplaceAttributeModifierPtr GetAttributeModifier() const { return mAttrModifier; }
    index::InplaceIndexModifier* GetIndexModifier() const { return mIndexModifier.get(); }
    PartitionModifierDumpTaskItemPtr GetDumpTaskItem(const PartitionModifierPtr& modifier) const override;
    std::vector<index::AttributeUpdateBitmapPtr> GetPackAttrUpdataBitmapVec() const;

private:
    void InitBuildingSegmentModifier(const index_base::InMemorySegmentPtr& inMemSegment);
    docid_t CalculateRealtimeSegmentBaseDocId(const index_base::PartitionDataPtr& partitionData);

private:
    index::PrimaryKeyIndexReaderPtr mPkIndexReader;

    // built segments
    index::DeletionMapReaderPtr mDeletionmapReader;
    index::InplaceAttributeModifierPtr mAttrModifier;
    std::unique_ptr<index::InplaceIndexModifier> mIndexModifier;
    docid_t mRealtimeSegmentBaseDocId;

    // building segment
    docid_t mBuildingSegmentBaseDocid;
    InMemorySegmentModifierPtr mBuildingSegmentModifier;
    IndexPartitionReaderPtr mPartitionReader;
    future_lite::Executor* mBuildExecutor;

private:
    friend class InplaceModifierTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InplaceModifier);
}} // namespace indexlib::partition
