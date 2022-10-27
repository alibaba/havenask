#ifndef __INDEXLIB_PACK_ATTRIBUTE_REWRITER_H
#define __INDEXLIB_PACK_ATTRIBUTE_REWRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/document/document_rewriter/multi_region_pack_attribute_appender.h"
#include "indexlib/config/index_partition_schema.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);

IE_NAMESPACE_BEGIN(document);

class PackAttributeRewriter : public document::DocumentRewriter
{
public:
    PackAttributeRewriter();
    ~PackAttributeRewriter();
public:
    bool Init(const config::IndexPartitionSchemaPtr& schema);
   
    void Rewrite(const document::DocumentPtr& doc) override;

private:
    void RewriteAttributeDocument(
        const MultiRegionPackAttributeAppenderPtr& appender,
        const document::NormalDocumentPtr& document);
    
private:
    MultiRegionPackAttributeAppenderPtr mMainDocAppender;
    MultiRegionPackAttributeAppenderPtr mSubDocAppender;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackAttributeRewriter);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_PACK_ATTRIBUTE_REWRITER_H
