#ifndef __INDEXLIB_DOCUMENT_REWRITER_H
#define __INDEXLIB_DOCUMENT_REWRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document.h"

IE_NAMESPACE_BEGIN(document);

class DocumentRewriter
{
public:
    DocumentRewriter() {}
    virtual ~DocumentRewriter() {}

public:
    virtual void Rewrite(const document::DocumentPtr& doc) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentRewriter);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_DOCUMENT_REWRITER_H
