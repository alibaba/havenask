#include "indexlib/document/document_rewriter/document_rewriter_chain.h"
#include "indexlib/document/document_rewriter/document_rewriter_creator.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, DocumentRewriterChain);

DocumentRewriterChain::DocumentRewriterChain(
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const common::SortDescriptions& sortDesc)
    : mSchema(schema)
    , mOptions(options)
    , mSortDescriptions(sortDesc)
{
    assert(schema);
}

DocumentRewriterChain::~DocumentRewriterChain() 
{
}

void DocumentRewriterChain::Init()
{
    mRewriters.clear();
    if (mSchema->GetTableType() == tt_kkv || mSchema->GetTableType() == tt_kv)
    {
        PushDeleteToAddDocumentRewriter();
        PushPackAttributeRewriter();
    }
    else
    {
        PushAddToUpdateRewriter();
        PushTimestampRewriter();
        PushSectionAttributeRewriter();
        PushPackAttributeRewriter();
    }
}

void DocumentRewriterChain::Rewrite(const DocumentPtr& doc)
{
    for (size_t i = 0; i < mRewriters.size(); i++)
    {
        mRewriters[i]->Rewrite(doc);
    }
}

void DocumentRewriterChain::PushAddToUpdateRewriter()
{
    DocumentRewriterPtr add2UpdateRewriter(
            DocumentRewriterCreator::CreateAddToUpdateDocumentRewriter(
                    mSchema, mOptions, mSortDescriptions));
    if (add2UpdateRewriter)
    {
        mRewriters.push_back(add2UpdateRewriter);
    }
}

void DocumentRewriterChain::PushTimestampRewriter()
{
    DocumentRewriterPtr timestampRewriter(
            DocumentRewriterCreator::CreateTimestampRewriter(mSchema));
    if (timestampRewriter)
    {
        mRewriters.push_back(timestampRewriter);
    }
}

void DocumentRewriterChain::PushSectionAttributeRewriter()
{
    // todo: only be used for lagacy document version
    // delete after DOCUMENT_BINARY_VERSION is 3 (current is 2)
    DocumentRewriterPtr sectionAttributeRewriter(
            DocumentRewriterCreator::CreateSectionAttributeRewriter(mSchema));
    if (sectionAttributeRewriter)
    {
        mRewriters.push_back(sectionAttributeRewriter);
    }
}

void DocumentRewriterChain::PushPackAttributeRewriter()
{
    DocumentRewriterPtr packAttributeRewriter(
            DocumentRewriterCreator::CreatePackAttributeRewriter(mSchema));
    if (packAttributeRewriter)
    {
        mRewriters.push_back(packAttributeRewriter);
    }
}

void DocumentRewriterChain::PushDeleteToAddDocumentRewriter()
{
    DocumentRewriterPtr rewriter(
            DocumentRewriterCreator::CreateDeleteToAddDocumentRewriter());
    assert(rewriter);
    mRewriters.push_back(rewriter);
}

IE_NAMESPACE_END(document);

