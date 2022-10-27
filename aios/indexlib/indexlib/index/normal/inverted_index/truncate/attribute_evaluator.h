#ifndef __INDEXLIB_ATTRIBUTE_EVALUATOR_H
#define __INDEXLIB_ATTRIBUTE_EVALUATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/normal/inverted_index/truncate/evaluator.h"
#include "indexlib/index/normal/inverted_index/truncate/reference_typed.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

template <typename T>
class AttributeEvaluator : public Evaluator
{
public:
    AttributeEvaluator(AttributeSegmentReaderPtr attrReader, Reference* refer)
    {
        mRefer = dynamic_cast<ReferenceTyped<T>* >(refer);
        assert(mRefer);
        mAttrReader = attrReader;
    }
    ~AttributeEvaluator() {}

public:
    void Evaluate(docid_t docId, 
                  const PostingIteratorPtr& postingIter,
                  DocInfo *docInfo);

private:
    ReferenceTyped<T>* mRefer;
    AttributeSegmentReaderPtr mAttrReader;

private:
    IE_LOG_DECLARE();
};

template <typename T>
inline void AttributeEvaluator<T>::Evaluate(docid_t docId, 
        const PostingIteratorPtr& postingIter,
        DocInfo *docInfo)
{
    T attrValue;
    mAttrReader->Read(docId, (uint8_t *)&attrValue, sizeof(T));
    mRefer->Set(attrValue, docInfo);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_EVALUATOR_H
