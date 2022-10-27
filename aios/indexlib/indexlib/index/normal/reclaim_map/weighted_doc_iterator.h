#ifndef __INDEXLIB_WEIGHTED_DOC_ITERATOR_H
#define __INDEXLIB_WEIGHTED_DOC_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/attribute_evaluator.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"
#include "indexlib/index/normal/inverted_index/truncate/comparator.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"

IE_NAMESPACE_BEGIN(index);

class WeightedDocIterator
{
public:
    WeightedDocIterator(
            const index_base::SegmentMergeInfo& segMergeInfo,
            EvaluatorPtr evaluatorPtr,
            ComparatorPtr comparatorPtr,
            DocInfoAllocatorPtr docInfoAllocator)
        : mSegMergeInfo(segMergeInfo)
        , mEvaluatorPtr(evaluatorPtr)
        , mComparatorPtr(comparatorPtr)
        , mDocInfoAllocator(docInfoAllocator)
    {
        mDocInfo = mDocInfoAllocator->Allocate();
        mDocInfo->SetDocId(-1);
        mMaxDocId = (docid_t)(mSegMergeInfo.segmentInfo.docCount - 1);
    }

    ~WeightedDocIterator()
    {
        mDocInfoAllocator->Deallocate(mDocInfo);
    }

    bool HasNext() const
    {
        return mDocInfo->GetDocId() < mMaxDocId;
    }

    docid_t Next()
    {
        if (mDocInfo->GetDocId() < mMaxDocId)
        {
            docid_t docId = mDocInfo->IncDocId();
            mEvaluatorPtr->Evaluate(docId, PostingIteratorPtr(), mDocInfo);
            return docId;
        }
        return INVALID_DOCID;
    }

    docid_t GetLocalDocId() const
    {
        return mDocInfo->GetDocId();
    }

    docid_t GetBasedocId() const
    {
        return mSegMergeInfo.baseDocId;
    }

    const ComparatorPtr& GetComparator() const
    {
        return mComparatorPtr;
    }

    segmentid_t GetSegmentId() const
    {
        return mSegMergeInfo.segmentId;
    }

    DocInfo* GetDocInfo() const
    {
        return mDocInfo;
    }

private:
    DocInfo *mDocInfo;
    docid_t mMaxDocId;
    index_base::SegmentMergeInfo mSegMergeInfo;
    EvaluatorPtr mEvaluatorPtr;
    ComparatorPtr mComparatorPtr;
    DocInfoAllocatorPtr mDocInfoAllocator;
private:
    IE_LOG_DECLARE();
};

class WeightedDocIteratorComp
{
public:
    bool operator() (WeightedDocIterator* left, WeightedDocIterator* right) const
    {
        ComparatorPtr cmp = left->GetComparator();
        if (cmp->LessThan(right->GetDocInfo(), left->GetDocInfo()))
        {
            return true;
        }
        else if (cmp->LessThan(left->GetDocInfo(), right->GetDocInfo()))
        {
            return false;
        }
        return left->GetSegmentId() > right->GetSegmentId();
    }
};

DEFINE_SHARED_PTR(WeightedDocIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_WEIGHTED_DOC_ITERATOR_H
