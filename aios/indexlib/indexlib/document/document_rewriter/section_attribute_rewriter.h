#ifndef __INDEXLIB_SECTION_ATTRIBUTE_REWRITER_H
#define __INDEXLIB_SECTION_ATTRIBUTE_REWRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document_rewriter/section_attribute_appender.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);

IE_NAMESPACE_BEGIN(document);

class SectionAttributeRewriter : public document::DocumentRewriter
{
public:
    SectionAttributeRewriter();
    ~SectionAttributeRewriter();
public:
    bool Init(const config::IndexPartitionSchemaPtr& schema);
    void Rewrite(const document::DocumentPtr& doc) override;

private:
    void RewriteIndexDocument(
	const SectionAttributeAppenderPtr& appender,
	const document::NormalDocumentPtr& document);
    
private:
    SectionAttributeAppenderPtr mMainDocAppender;
    SectionAttributeAppenderPtr mSubDocAppender;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SectionAttributeRewriter);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_SECTION_ATTRIBUTE_REWRITER_H
