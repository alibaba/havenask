#include "indexlib/partition/document_deduper/sub_doc_built_segments_document_deduper.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SubDocBuiltSegmentsDocumentDeduper);

SubDocBuiltSegmentsDocumentDeduper::SubDocBuiltSegmentsDocumentDeduper(const config::IndexPartitionSchemaPtr& schema)
    : DocumentDeduper(schema) {}

void SubDocBuiltSegmentsDocumentDeduper::Init(const partition::PartitionModifierPtr& modifier)
{
    partition::SubDocModifierPtr subDocModifier = 
        DYNAMIC_POINTER_CAST(partition::SubDocModifier, modifier);
    assert(subDocModifier);

    mMainSegmentsDeduper.reset(new BuiltSegmentsDocumentDeduper(mSchema));
    //subDocModifier get pkReader is mainModifier pkReader
    //subDocModifier remove mainDupPk, can also remove its subDupPk
    mMainSegmentsDeduper->Init(subDocModifier);

    mSubSegmentsDeduper.reset(new BuiltSegmentsDocumentDeduper(
                                  mSchema->GetSubIndexPartitionSchema()));
    mSubSegmentsDeduper->Init(subDocModifier->GetSubModifier());
}

void SubDocBuiltSegmentsDocumentDeduper::Dedup()
{
    mMainSegmentsDeduper->Dedup();
    mSubSegmentsDeduper->Dedup();
}

IE_NAMESPACE_END(partition);

