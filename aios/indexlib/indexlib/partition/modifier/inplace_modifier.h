#ifndef __INDEXLIB_INPLACE_MODIFIER_H
#define __INDEXLIB_INPLACE_MODIFIER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/segment/in_memory_segment_modifier.h"
#include "indexlib/partition/index_partition_reader.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, InplaceAttributeModifier);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);

IE_NAMESPACE_BEGIN(partition);

class InplaceModifier : public PartitionModifier
{
public:
    InplaceModifier(const config::IndexPartitionSchemaPtr& schema);
    ~InplaceModifier();

public:
    void Init(const IndexPartitionReaderPtr& partitionReader,
              const index_base::InMemorySegmentPtr& inMemSegment,
              const util::BuildResourceMetricsPtr& buildResourceMetrics
              = util::BuildResourceMetricsPtr());

    bool DedupDocument(const document::DocumentPtr& doc) override
    { return RemoveDocument(doc); }
    bool UpdateDocument(const document::DocumentPtr& doc) override;
    bool RemoveDocument(const document::DocumentPtr& doc) override;
    void Dump(const file_system::DirectoryPtr& directory,
                            segmentid_t srcSegmentId) override;
    bool IsDirty() const override;
    bool UpdateField(docid_t docId, fieldid_t fieldId, 
                                   const autil::ConstString& value) override;
    
    bool UpdatePackField(docid_t docId, packattrid_t packAttrId, 
                                       const autil::ConstString& value) override;
        
    bool RemoveDocument(docid_t docId) override;
    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyIndexReader() const override
    { return mPkIndexReader; }
    const index::PartitionInfoPtr& GetPartitionInfo() const override
    { return mPartitionInfo; }

    index::DeletionMapReaderPtr GetDeletionMapReader() const
    {
        return mDeletionmapReader;
    }
    index::InplaceAttributeModifierPtr GetAttributeModifier() const
    {
        return mAttrModifier;
    }

private:
    void InitBuildingSegmentModifier(const index_base::InMemorySegmentPtr& inMemSegment);
    docid_t CalculateBuildingSegmentBaseDocId(
            const index_base::PartitionDataPtr& partitionData);
    
private:
    index::PrimaryKeyIndexReaderPtr mPkIndexReader;

    //built segments
    index::DeletionMapReaderPtr mDeletionmapReader;
    index::InplaceAttributeModifierPtr mAttrModifier;

    // building segment
    docid_t mBuildingSegmentBaseDocid;
    InMemorySegmentModifierPtr mBuildingSegmentModifier;
    index::PartitionInfoPtr mPartitionInfo;

private:
    friend class InplaceModifierTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InplaceModifier);
IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INPLACE_MODIFIER_H
