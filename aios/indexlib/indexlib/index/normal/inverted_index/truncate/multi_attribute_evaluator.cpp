#include "indexlib/index/normal/inverted_index/truncate/multi_attribute_evaluator.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiAttributeEvaluator);

MultiAttributeEvaluator::MultiAttributeEvaluator() 
{
}

MultiAttributeEvaluator::~MultiAttributeEvaluator() 
{
}

void MultiAttributeEvaluator::AddEvaluator(const EvaluatorPtr& evaluator)
{
    mEvaluators.push_back(evaluator);
}

void MultiAttributeEvaluator::Evaluate(docid_t docId, 
                                       const PostingIteratorPtr& postingIter,
                                       DocInfo *docInfo)
{
    for (size_t i = 0; i < mEvaluators.size(); i++)
    {
        mEvaluators[i]->Evaluate(docId, postingIter, docInfo);
    }
}

IE_NAMESPACE_END(index);

