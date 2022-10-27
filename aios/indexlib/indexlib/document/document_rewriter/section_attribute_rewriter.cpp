#include "indexlib/document/document_rewriter/section_attribute_rewriter.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, SectionAttributeRewriter);

SectionAttributeRewriter::SectionAttributeRewriter() 
{
}

SectionAttributeRewriter::~SectionAttributeRewriter() 
{
}

bool SectionAttributeRewriter::Init(const IndexPartitionSchemaPtr& schema)
{
    SectionAttributeAppenderPtr mainAppender(new SectionAttributeAppender());
    if (mainAppender->Init(schema))
    {
	mMainDocAppender = mainAppender;
    }
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
	SectionAttributeAppenderPtr subAppender(new SectionAttributeAppender());
	if (subAppender->Init(subSchema))
	{
	    mSubDocAppender = subAppender;
	}	
    }
    return mMainDocAppender || mSubDocAppender;
}

void SectionAttributeRewriter::Rewrite(const DocumentPtr& document)
{
    if (document->GetDocOperateType() != ADD_DOC)
    {
        return;
    }
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    if (mMainDocAppender)
    {
	RewriteIndexDocument(mMainDocAppender, doc);
    }

    if (mSubDocAppender)
    {
	const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
	for (size_t i = 0; i < subDocs.size(); ++i)
	{
	    RewriteIndexDocument(mSubDocAppender, subDocs[i]);
	}
    }
}

void SectionAttributeRewriter::RewriteIndexDocument(
    const SectionAttributeAppenderPtr& appender,
    const NormalDocumentPtr& document)
{
    const IndexDocumentPtr& indexDocument = document->GetIndexDocument();
    if (!indexDocument)
    {
	INDEXLIB_FATAL_ERROR(UnSupported, "indexDocument is NULL!");
    }
    appender->AppendSectionAttribute(indexDocument);
}


IE_NAMESPACE_END(document);

