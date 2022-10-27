#ifndef __INDEXLIB_DOCUMENT_DEDUPER_CREATOR_H
#define __INDEXLIB_DOCUMENT_DEDUPER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/document_deduper/document_deduper.h"
#include "indexlib/partition/modifier/partition_modifier.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);

IE_NAMESPACE_BEGIN(partition);

class DocumentDeduperCreator
{
public:
    DocumentDeduperCreator();
    ~DocumentDeduperCreator();
public:
    static DocumentDeduperPtr CreateBuildingSegmentDeduper(
            const config::IndexPartitionSchemaPtr& schema,
            const config::IndexPartitionOptions& options,
            const index_base::InMemorySegmentPtr& inMemSegment,
            const partition::PartitionModifierPtr& modifier);

    static DocumentDeduperPtr CreateBuiltSegmentsDeduper(
            const config::IndexPartitionSchemaPtr& schema,
            const config::IndexPartitionOptions& options,
            const partition::PartitionModifierPtr& modifier);

private:
    static bool CanCreateDeduper(const config::IndexPartitionSchemaPtr& schema,
                                 const config::IndexPartitionOptions& options,
                                 const partition::PartitionModifierPtr& modifier);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentDeduperCreator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_DOCUMENT_DEDUPER_CREATOR_H
