#include "indexlib/common/document_deduper_helper.h"
#include "indexlib/partition/document_deduper/document_deduper_creator.h"
#include "indexlib/partition/document_deduper/building_segment_document_deduper.h"
#include "indexlib/partition/document_deduper/built_segments_document_deduper.h"
#include "indexlib/partition/document_deduper/sub_doc_built_segments_document_deduper.h"
#include "indexlib/partition/document_deduper/sub_doc_building_segment_document_deduper.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, DocumentDeduperCreator);

DocumentDeduperCreator::DocumentDeduperCreator() 
{
}

DocumentDeduperCreator::~DocumentDeduperCreator() 
{
}

bool DocumentDeduperCreator::CanCreateDeduper(
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const PartitionModifierPtr& modifier)
{
    if (!modifier)
    {
        return false;
    }

    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    return common::DocumentDeduperHelper::DelayDedupDocument(
            options, indexSchema->GetPrimaryKeyIndexConfig());
}

DocumentDeduperPtr DocumentDeduperCreator::CreateBuildingSegmentDeduper(
        const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
        const InMemorySegmentPtr& inMemSegment, const PartitionModifierPtr& modifier)
{
    if (!CanCreateDeduper(schema, options, modifier))
    {
        return DocumentDeduperPtr();
    }

    assert(inMemSegment);
    if (schema->GetSubIndexPartitionSchema())
    {
        SubDocBuildingSegmentDocumentDeduperPtr deduper(
                new SubDocBuildingSegmentDocumentDeduper(schema));
        deduper->Init(inMemSegment, modifier);
        return deduper;
    }

    BuildingSegmentDocumentDeduperPtr deduper(
            new BuildingSegmentDocumentDeduper(schema));
    deduper->Init(inMemSegment, modifier);
    return deduper;
}

DocumentDeduperPtr DocumentDeduperCreator::CreateBuiltSegmentsDeduper(
            const IndexPartitionSchemaPtr& schema,
            const IndexPartitionOptions& options,
            const PartitionModifierPtr& modifier)
{
    if (!CanCreateDeduper(schema, options, modifier))
    {
        return DocumentDeduperPtr();
    }

    if (schema->GetSubIndexPartitionSchema())
    {
        SubDocBuiltSegmentsDocumentDeduperPtr deduper(
                new SubDocBuiltSegmentsDocumentDeduper(schema));
        deduper->Init(modifier);
        return deduper;
    }

    BuiltSegmentsDocumentDeduperPtr deduper(
            new BuiltSegmentsDocumentDeduper(schema));
    deduper->Init(modifier);
    return deduper;
}

IE_NAMESPACE_END(partition);

