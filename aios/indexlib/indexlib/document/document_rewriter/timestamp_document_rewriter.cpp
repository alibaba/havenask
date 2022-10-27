#include <autil/StringUtil.h>
#include "indexlib/document/document_rewriter/timestamp_document_rewriter.h"
#include "indexlib/util/key_hasher_typed.h"
#include "indexlib/document/index_document/normal_document/field.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, TimestampDocumentRewriter);

TimestampDocumentRewriter::TimestampDocumentRewriter(
        fieldid_t timestampFieldId) 
    : mTimestampFieldId(timestampFieldId)
{
}

TimestampDocumentRewriter::~TimestampDocumentRewriter() 
{
}

void TimestampDocumentRewriter::Rewrite(const DocumentPtr& document)
{
    DocOperateType opType = document->GetDocOperateType();
    if (opType != ADD_DOC)
    {
        return;
    }
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    string timestamp = StringUtil::toString(doc->GetTimestamp());
    IndexDocumentPtr indexDoc = doc->GetIndexDocument();
    if (!indexDoc)
    {
        return;
    }
    
    Field *field = indexDoc->GetField(mTimestampFieldId);
    if (field)
    {
        IE_INTERVAL_LOG2(60, INFO, "field [%d] will be reset for timestamp field rewrite.", mTimestampFieldId);
        indexDoc->ClearField(mTimestampFieldId);
    }
    
    field = indexDoc->CreateField(mTimestampFieldId, Field::FieldTag::TOKEN_FIELD);
    assert(field);    
    auto tokenizeField = dynamic_cast<IndexTokenizeField*>(field);
    assert(tokenizeField);
    tokenizeField->Reset();
    tokenizeField->SetFieldId(mTimestampFieldId);
    Section* section = tokenizeField->CreateSection(1);
    Int64NumberHasher hasher;
    dictkey_t hashKey;
    hasher.GetHashKey(timestamp.data(), hashKey);
    section->CreateToken(hashKey);
}

IE_NAMESPACE_END(document);

