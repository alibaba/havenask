#ifndef __INDEXLIB_DOC_COLLECTOR_H
#define __INDEXLIB_DOC_COLLECTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_common.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_vector_allocator.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_filter_processor.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_distinctor.h"

IE_NAMESPACE_BEGIN(index);

class DocCollector
{
public:
    typedef BucketVectorAllocator::DocIdVector DocIdVector;
    typedef BucketVectorAllocator::DocIdVectorPtr DocIdVectorPtr;
    typedef BucketVectorAllocator::BucketVector BucketVector;
    typedef BucketVectorAllocator::BucketVectorPtr BucketVectorPtr;
public:
    DocCollector(uint64_t minDocCountToReserve, 
                 uint64_t maxDocCountToReserve,
                 const DocFilterProcessorPtr& filterProcessor,
                 const DocDistinctorPtr& docDistinctor,
                 const BucketVectorAllocatorPtr &bucketVecAllocator)
        : mMinDocCountToReserve(minDocCountToReserve)
        , mMaxDocCountToReserve(maxDocCountToReserve)
        , mFilterProcessor(filterProcessor)
        , mDocDistinctor(docDistinctor)
        , mBucketVecAllocator(bucketVecAllocator)
    {
    }
    virtual ~DocCollector() {}

public:
    void CollectDocIds(dictkey_t key, const PostingIteratorPtr& postingIt,
                       df_t docFreq = -1)
    {
        assert(postingIt);
        int64_t docIdVecReserveSize = (int64_t)mMinDocCountToReserve;

        if (docFreq >= 0 && docFreq < docIdVecReserveSize)
        {
            docIdVecReserveSize = docFreq;
        }
        mDocIdVec = mBucketVecAllocator->AllocateDocVector();
        mDocIdVec->clear();
        mDocIdVec->reserve(docIdVecReserveSize);
        mMinValueDocId = INVALID_DOCID;
        if (mFilterProcessor)
        {
            mFilterProcessor->BeginFilter(key, postingIt);
        }
        ReInit();
        DoCollect(key, postingIt);
        Truncate();
        if (mDocDistinctor)
        {
            mDocDistinctor->Reset();
        }
    }
    virtual int64_t EstimateMemoryUse(uint32_t docCount) const
    {
        int64_t size = sizeof(docid_t) * mMaxDocCountToReserve * 2; // mDocIdVec
        if (mDocDistinctor)
        {
            size += mDocDistinctor->EstimateMemoryUse(mMaxDocCountToReserve, docCount);
        }
        return size;
    }
private:
    virtual void ReInit() = 0;
    virtual void DoCollect(dictkey_t key, const PostingIteratorPtr& postingIt) = 0;
    virtual void Truncate() = 0;
public:
    virtual void Reset()
    {
        if (mDocIdVec)
        {
            mBucketVecAllocator->DeallocateDocIdVec(mDocIdVec);
            mDocIdVec.reset();
        }
    }
    bool Empty() const { return mDocIdVec->size() == 0; }
    const DocIdVectorPtr &GetTruncateDocIds() const { return mDocIdVec; }
    docid_t GetMinValueDocId() const { return mMinValueDocId; }
protected:
    uint64_t mMinDocCountToReserve;
    uint64_t mMaxDocCountToReserve;
    DocFilterProcessorPtr mFilterProcessor;
    DocDistinctorPtr mDocDistinctor;
    DocIdVectorPtr mDocIdVec;
    docid_t mMinValueDocId;
    BucketVectorAllocatorPtr mBucketVecAllocator;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocCollector);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_COLLECTOR_H
