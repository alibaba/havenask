#ifndef __INDEXLIB_MULTI_ATTRIBUTE_EVALUATOR_H
#define __INDEXLIB_MULTI_ATTRIBUTE_EVALUATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/evaluator.h"

IE_NAMESPACE_BEGIN(index);

class MultiAttributeEvaluator : public Evaluator
{
public:
    MultiAttributeEvaluator();
    ~MultiAttributeEvaluator();
public:
    void AddEvaluator(const EvaluatorPtr& evaluator);
    void Evaluate(docid_t docId, 
                  const PostingIteratorPtr& postingIter,
                  DocInfo *docInfo);

private:
    typedef std::vector<EvaluatorPtr> EvaluatorVec;
    EvaluatorVec mEvaluators;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiAttributeEvaluator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_ATTRIBUTE_EVALUATOR_H
