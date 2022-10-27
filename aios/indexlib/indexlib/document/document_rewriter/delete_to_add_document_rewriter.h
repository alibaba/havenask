#ifndef __INDEXLIB_DELETE_TO_ADD_DOCUMENT_REWRITER_H
#define __INDEXLIB_DELETE_TO_ADD_DOCUMENT_REWRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/config/kkv_index_config.h"

IE_NAMESPACE_BEGIN(document);

class DeleteToAddDocumentRewriter : public document::DocumentRewriter
{
public:
    DeleteToAddDocumentRewriter()
    {}
    ~DeleteToAddDocumentRewriter()
    {}
    
public:
    void Rewrite(const document::DocumentPtr& doc) override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DeleteToAddDocumentRewriter);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_KKV_DOCUMENT_REWRITER_H
