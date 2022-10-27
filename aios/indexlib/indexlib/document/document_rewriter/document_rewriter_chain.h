#ifndef __INDEXLIB_DOCUMENT_REWRITER_CHAIN_H
#define __INDEXLIB_DOCUMENT_REWRITER_CHAIN_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/common/sort_description.h"

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, DocumentRewriter);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(document);

class DocumentRewriterChain
{
public:
    DocumentRewriterChain(const config::IndexPartitionSchemaPtr& schema,
                          const config::IndexPartitionOptions& options,
                          const common::SortDescriptions& sortDesc =
                          common::SortDescriptions());
    
    ~DocumentRewriterChain();

public:
    void Init();
    void Rewrite(const document::DocumentPtr& doc);

private:
    void PushAddToUpdateRewriter();
    void PushTimestampRewriter();
    void PushSectionAttributeRewriter();
    void PushPackAttributeRewriter();
    void PushDeleteToAddDocumentRewriter();
private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    common::SortDescriptions mSortDescriptions;
    std::vector<document::DocumentRewriterPtr> mRewriters;

private:
    friend class DocumentRewriterChainTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentRewriterChain);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_DOCUMENT_REWRITER_CHAIN_H
