#ifndef __INDEXLIB_SUB_DOC_BUILDING_SEGMENT_DOCUMENT_DEDUPER_H
#define __INDEXLIB_SUB_DOC_BUILDING_SEGMENT_DOCUMENT_DEDUPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/document_deduper/document_deduper.h"
#include "indexlib/partition/document_deduper/building_segment_document_deduper.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"

IE_NAMESPACE_BEGIN(partition);

class SubDocBuildingSegmentDocumentDeduper : public DocumentDeduper
{
public:
    SubDocBuildingSegmentDocumentDeduper(const config::IndexPartitionSchemaPtr& schema)
        : DocumentDeduper(schema)
    {}
    ~SubDocBuildingSegmentDocumentDeduper() {}
public:
    void Init(const index_base::InMemorySegmentPtr& buildingSegment,
              const partition::PartitionModifierPtr& modifier)
    {
        partition::SubDocModifierPtr subDocModifier = 
            DYNAMIC_POINTER_CAST(partition::SubDocModifier, modifier);
        assert(subDocModifier);

        mMainDeduper.reset(new BuildingSegmentDocumentDeduper(mSchema));
        //subDocModifier get pkReader is mainModifier pkReader
        //subDocModifier remove mainDupPk, can also remove its subDupPk
        mMainDeduper->Init(buildingSegment, subDocModifier);

        mSubDeduper.reset(new BuildingSegmentDocumentDeduper(
                        mSchema->GetSubIndexPartitionSchema()));
        mSubDeduper->Init(buildingSegment->GetSubInMemorySegment(),
                          subDocModifier->GetSubModifier());
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

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SUB_DOC_BUILDING_SEGMENT_DOCUMENT_DEDUPER_H
