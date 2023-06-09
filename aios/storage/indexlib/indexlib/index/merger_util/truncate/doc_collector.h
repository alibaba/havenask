/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_DOC_COLLECTOR_H
#define __INDEXLIB_DOC_COLLECTOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/merger_util/truncate/bucket_vector_allocator.h"
#include "indexlib/index/merger_util/truncate/doc_distinctor.h"
#include "indexlib/index/merger_util/truncate/doc_filter_processor.h"
#include "indexlib/index/merger_util/truncate/truncate_common.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class DocCollector
{
public:
    typedef BucketVectorAllocator::DocIdVector DocIdVector;
    typedef BucketVectorAllocator::DocIdVectorPtr DocIdVectorPtr;
    typedef BucketVectorAllocator::BucketVector BucketVector;
    typedef BucketVectorAllocator::BucketVectorPtr BucketVectorPtr;

public:
    DocCollector(uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                 const DocFilterProcessorPtr& filterProcessor, const DocDistinctorPtr& docDistinctor,
                 const BucketVectorAllocatorPtr& bucketVecAllocator)
        : mMinDocCountToReserve(minDocCountToReserve)
        , mMaxDocCountToReserve(maxDocCountToReserve)
        , mFilterProcessor(filterProcessor)
        , mDocDistinctor(docDistinctor)
        , mBucketVecAllocator(bucketVecAllocator)
    {
    }
    virtual ~DocCollector() {}

public:
    void CollectDocIds(const index::DictKeyInfo& key, const PostingIteratorPtr& postingIt, df_t docFreq = -1,
                       int64_t* minValue = nullptr, int64_t* maxValue = nullptr)
    {
        assert(postingIt);
        int64_t docIdVecReserveSize = (int64_t)mMinDocCountToReserve;

        if (docFreq >= 0 && docFreq < docIdVecReserveSize) {
            docIdVecReserveSize = docFreq;
        }
        mDocIdVec = mBucketVecAllocator->AllocateDocVector();
        mDocIdVec->clear();
        mDocIdVec->reserve(docIdVecReserveSize);
        mMinValueDocId = INVALID_DOCID;
        if (mFilterProcessor) {
            if (minValue and maxValue) {
                mFilterProcessor->BeginFilter(*minValue, *maxValue, postingIt);
            } else {
                mFilterProcessor->BeginFilter(key, postingIt);
            }
        }
        ReInit();
        DoCollect(key, postingIt);
        Truncate();
        if (mDocDistinctor) {
            mDocDistinctor->Reset();
        }
    }

    virtual int64_t EstimateMemoryUse(uint32_t docCount) const
    {
        int64_t size = sizeof(docid_t) * mMaxDocCountToReserve * 2; // mDocIdVec
        if (mDocDistinctor) {
            size += mDocDistinctor->EstimateMemoryUse(mMaxDocCountToReserve, docCount);
        }
        return size;
    }

private:
    virtual void ReInit() = 0;
    virtual void DoCollect(const index::DictKeyInfo& key, const PostingIteratorPtr& postingIt) = 0;
    virtual void Truncate() = 0;

public:
    virtual void Reset()
    {
        if (mDocIdVec) {
            mBucketVecAllocator->DeallocateDocIdVec(mDocIdVec);
            mDocIdVec.reset();
        }
    }
    bool Empty() const { return mDocIdVec->size() == 0; }
    const DocIdVectorPtr& GetTruncateDocIds() const { return mDocIdVec; }
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
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_DOC_COLLECTOR_H
