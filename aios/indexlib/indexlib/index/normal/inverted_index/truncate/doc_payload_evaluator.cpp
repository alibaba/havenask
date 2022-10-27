#include "indexlib/index/normal/inverted_index/truncate/doc_payload_evaluator.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DocPayloadEvaluator);

DocPayloadEvaluator::DocPayloadEvaluator() 
{
}

DocPayloadEvaluator::~DocPayloadEvaluator() 
{
}

void DocPayloadEvaluator::Init(Reference* refer)
{
    mRefer = dynamic_cast<ReferenceTyped<docpayload_t>* >(refer);
}

void DocPayloadEvaluator::Evaluate(docid_t docId,
                                   const PostingIteratorPtr &postingIter,
                                   DocInfo *docInfo) 
{
    if (postingIter->SeekDoc(docId) != INVALID_DOCID)
    {
        docpayload_t docPayload = postingIter->GetDocPayload();
        mRefer->Set(docPayload, docInfo);
    }
}

IE_NAMESPACE_END(index);

