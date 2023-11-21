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
#include "indexlib/partition/modifier/partition_modifier.h"

DECLARE_REFERENCE_CLASS(partition, InMemorySegmentModifier);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter)
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index, DeletionMapWriter);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, PatchAttributeModifier);
DECLARE_REFERENCE_CLASS(index, PatchIndexModifier);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, ModifiedTokens);

namespace indexlib { namespace partition {

class PatchModifier : public PartitionModifier
{
public:
    PatchModifier(const config::IndexPartitionSchemaPtr& schema, bool enablePackFile = false, bool isOffline = false);

    ~PatchModifier();

public:
    virtual void Init(const index_base::PartitionDataPtr& partitionData,
                      const util::BuildResourceMetricsPtr& buildResourceMetrics = util::BuildResourceMetricsPtr());

    bool DedupDocument(const document::DocumentPtr& doc) override;
    bool UpdateDocument(const document::DocumentPtr& doc) override;
    bool RemoveDocument(const document::DocumentPtr& doc) override;
    bool RemoveDocument(docid_t docId) override;

    void Dump(const file_system::DirectoryPtr& directory, segmentid_t srcSegmentId) override;

    bool IsDirty() const override;

    bool UpdateField(docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull) override;

    bool RedoIndex(docid_t docId, const document::ModifiedTokens& modifiedTokens) override;

    docid_t GetBuildingSegmentBaseDocId() const override { return mBuildingSegmentBaseDocid; }
    index::PartitionInfoPtr GetPartitionInfo() const override;

    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyIndexReader() const override { return mPkIndexReader; }
    index::DeletionMapWriterPtr GetDeletionMapWriter() const { return mDeletionmapWriter; }

    // for offline, DumpTaskItem will hold the reference of modifier util it has been dumpped
    PartitionModifierDumpTaskItemPtr GetDumpTaskItem(const PartitionModifierPtr& modifier) const override;

public:
    // Unimplemented methods.
    bool UpdatePackField(docid_t docId, packattrid_t packAttrId, const autil::StringView& value) override;
    bool UpdateIndex(index::IndexUpdateTermIterator* iterator) override;

private:
    // virtual for test
    virtual index::PrimaryKeyIndexReaderPtr
    LoadPrimaryKeyIndexReader(const index_base::PartitionDataPtr& partitionData);

    void InitDeletionMapWriter(const index_base::PartitionDataPtr& partitionData);
    void InitAttributeModifier(const index_base::PartitionDataPtr& partitionData);
    void InitIndexModifier(const index_base::PartitionDataPtr& partitionData);

    void InitBuildingSegmentModifier(const index_base::InMemorySegmentPtr& inMemSegment);

    void UpdateInMemorySegmentInfo();

private:
    index::PrimaryKeyIndexReaderPtr mPkIndexReader;

    // built segments
    index::DeletionMapWriterPtr mDeletionmapWriter;
    index::PatchAttributeModifierPtr mAttrModifier;
    std::unique_ptr<index::PatchIndexModifier> mIndexModifier;

    // building segment
    docid_t mBuildingSegmentBaseDocid;
    InMemorySegmentModifierPtr mBuildingSegmentModifier;
    // hold SegmentWriter for InMemorySegmentModifier
    index_base::SegmentWriterPtr mSegmentWriter;

    index_base::PartitionDataPtr mPartitionData;
    bool mEnablePackFile;
    bool mIsOffline;

private:
    friend class PatchModifierTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchModifier);
}} // namespace indexlib::partition
