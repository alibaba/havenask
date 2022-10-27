#ifndef __INDEXLIB_DOC_PAYLOAD_EVALUATOR_H
#define __INDEXLIB_DOC_PAYLOAD_EVALUATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/evaluator.h"
#include "indexlib/index/normal/inverted_index/truncate/reference_typed.h"

IE_NAMESPACE_BEGIN(index);

class DocPayloadEvaluator : public Evaluator
{
public:
    DocPayloadEvaluator();
    ~DocPayloadEvaluator();

public:
    void Init(Reference* refer);

    void Evaluate(docid_t docId, 
                  const PostingIteratorPtr& postingIter,
                  DocInfo *docInfo);

private:
    ReferenceTyped<docpayload_t>* mRefer;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocPayloadEvaluator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_PAYLOAD_EVALUATOR_H
