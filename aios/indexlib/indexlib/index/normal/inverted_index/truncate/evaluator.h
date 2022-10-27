#ifndef __INDEXLIB_EVALUATOR_H
#define __INDEXLIB_EVALUATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info.h"

IE_NAMESPACE_BEGIN(index);

class Evaluator
{
public:
    Evaluator() {}
    virtual ~Evaluator() {}

public:
    virtual void Evaluate(docid_t docId, 
                          const PostingIteratorPtr& postingIter,
                          DocInfo *docInfo) = 0;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Evaluator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_EVALUATOR_H
