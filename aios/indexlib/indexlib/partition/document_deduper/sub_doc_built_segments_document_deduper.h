#ifndef __INDEXLIB_SUB_DOC_BUILT_SEGMENTS_DOCUMENT_DEDUPER_H
#define __INDEXLIB_SUB_DOC_BUILT_SEGMENTS_DOCUMENT_DEDUPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/partition/document_deduper/document_deduper.h"
#include "indexlib/partition/document_deduper/built_segments_document_deduper.h"

IE_NAMESPACE_BEGIN(partition);

class SubDocBuiltSegmentsDocumentDeduper : public DocumentDeduper
{
public:
    SubDocBuiltSegmentsDocumentDeduper(const config::IndexPartitionSchemaPtr& schema);

    ~SubDocBuiltSegmentsDocumentDeduper() {}

public:
    void Init(const partition::PartitionModifierPtr& modifier);

    void Dedup() override;

private:
    BuiltSegmentsDocumentDeduperPtr mMainSegmentsDeduper;
    BuiltSegmentsDocumentDeduperPtr mSubSegmentsDeduper;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocBuiltSegmentsDocumentDeduper);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SUB_DOC_BUILT_SEGMENTS_DOCUMENT_DEDUPER_H
