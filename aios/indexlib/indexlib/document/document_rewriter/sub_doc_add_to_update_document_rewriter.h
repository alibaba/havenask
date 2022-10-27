#ifndef __INDEXLIB_SUB_DOC_ADD_TO_UPDATE_DOCUMENT_REWRITER_H
#define __INDEXLIB_SUB_DOC_ADD_TO_UPDATE_DOCUMENT_REWRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/document/document_rewriter/add_to_update_document_rewriter.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);

IE_NAMESPACE_BEGIN(document);

class SubDocAddToUpdateDocumentRewriter : public document::DocumentRewriter
{
public:
    SubDocAddToUpdateDocumentRewriter();
    ~SubDocAddToUpdateDocumentRewriter();
public:
    void Init(const config::IndexPartitionSchemaPtr& schema,
              AddToUpdateDocumentRewriter* mainRewriter,
              AddToUpdateDocumentRewriter* subRewriter);

    void Rewrite(const document::DocumentPtr& doc) override;

private:
    bool ValidateModifiedField(const document::NormalDocumentPtr& doc) const;
    static bool IsExistSubFieldId(const document::NormalDocumentPtr& doc, fieldid_t fid);
    void ClearModifiedFields(const document::NormalDocumentPtr& doc);

private:
    AddToUpdateDocumentRewriter* mMainRewriter;
    AddToUpdateDocumentRewriter* mSubRewriter;
    fieldid_t mSubPkFieldId;

private:
    friend class SubDocAddToUpdateDocumentRewriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocAddToUpdateDocumentRewriter);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_SUB_DOC_ADD_TO_UPDATE_DOCUMENT_REWRITER_H
