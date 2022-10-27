#ifndef __INDEXLIB_PATCH_MODIFIER_H
#define __INDEXLIB_PATCH_MODIFIER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/modifier/partition_modifier.h"

DECLARE_REFERENCE_CLASS(partition, InMemorySegmentModifier);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter)
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index, DeletionMapWriter);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, PatchAttributeModifier);
DECLARE_REFERENCE_CLASS(document, Document);

IE_NAMESPACE_BEGIN(partition);

class PatchModifier : public PartitionModifier
{
public:
    PatchModifier(const config::IndexPartitionSchemaPtr& schema,
                  bool enablePackFile = false,
                  bool isOffline = false);

    ~PatchModifier();

public:
    virtual void Init(const index_base::PartitionDataPtr& partitionData,
                      const util::BuildResourceMetricsPtr& buildResourceMetrics
                      = util::BuildResourceMetricsPtr());

    bool DedupDocument(const document::DocumentPtr& doc) override;
    bool UpdateDocument(const document::DocumentPtr& doc) override;
    bool RemoveDocument(const document::DocumentPtr& doc) override;
    void Dump(
            const file_system::DirectoryPtr& directory, segmentid_t srcSegmentId) override;
    bool IsDirty() const override;

    bool UpdateField(docid_t docId, fieldid_t fieldId, 
                                   const autil::ConstString& value) override;

    bool UpdatePackField(docid_t docId, packattrid_t packAttrId, 
                                       const autil::ConstString& value) override
    { assert(false); return false; }
    
    bool RemoveDocument(docid_t docId) override;
    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyIndexReader() const override
    { return mPkIndexReader; }

    const index::PartitionInfoPtr& GetPartitionInfo() const override
    { return mPartitionInfo; }

    index::DeletionMapWriterPtr GetDeletionMapWriter() const
    {
        return mDeletionmapWriter;
    }

private:
    //virtual for test
    virtual index::PrimaryKeyIndexReaderPtr LoadPrimaryKeyIndexReader(
        const index_base::PartitionDataPtr& partitionData);

    void InitDeletionMapWriter(const index_base::PartitionDataPtr& partitionData);
    void InitAttributeModifier(const index_base::PartitionDataPtr& partitionData);

    void InitBuildingSegmentModifier(const index_base::InMemorySegmentPtr& inMemSegment);
    docid_t CalculateBuildingSegmentBaseDocId(
        const index_base::PartitionDataPtr& partitionData);

    void UpdateInMemorySegmentInfo();

private:
    index::PrimaryKeyIndexReaderPtr mPkIndexReader;

    // built segments
    index::DeletionMapWriterPtr mDeletionmapWriter;
    index::PatchAttributeModifierPtr mAttrModifier; 

    // building segment
    docid_t mBuildingSegmentBaseDocid;
    InMemorySegmentModifierPtr mBuildingSegmentModifier;
    // hold SegmentWriter for InMemorySegmentModifier
    index_base::SegmentWriterPtr mSegmentWriter;

    index_base::PartitionDataPtr mPartitionData;
    index::PartitionInfoPtr mPartitionInfo;
    bool mEnablePackFile;
    bool mIsOffline;
    
private:
    friend class PatchModifierTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchModifier);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PATCH_MODIFIER_H
