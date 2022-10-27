#include "indexlib/document/document_rewriter/delete_to_add_document_rewriter.h"
#include "indexlib/document/document.h"

using namespace std;

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, DeleteToAddDocumentRewriter);

void DeleteToAddDocumentRewriter::Rewrite(const DocumentPtr& doc)
{
    DocOperateType type = doc->GetDocOperateType();
    if (type == ADD_DOC || type == SKIP_DOC)
    {
        return;
    }
    
    if (type == DELETE_DOC)
    {
        doc->ModifyDocOperateType(ADD_DOC);
        return;
    }

    //TODO: supportupdate
    assert(false);
}
IE_NAMESPACE_END(document);

