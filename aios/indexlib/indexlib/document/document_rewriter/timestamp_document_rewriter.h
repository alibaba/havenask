#ifndef __INDEXLIB_TIMESTAMP_DOCUMENT_REWRITER_H
#define __INDEXLIB_TIMESTAMP_DOCUMENT_REWRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"

IE_NAMESPACE_BEGIN(document);

class TimestampDocumentRewriter : public document::DocumentRewriter
{
public:
    TimestampDocumentRewriter(fieldid_t timestampFieldId);
    ~TimestampDocumentRewriter();

public:
    void Rewrite(const document::DocumentPtr& doc) override;

private:
    fieldid_t mTimestampFieldId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TimestampDocumentRewriter);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_TIMESTAMP_DOCUMENT_REWRITER_H
