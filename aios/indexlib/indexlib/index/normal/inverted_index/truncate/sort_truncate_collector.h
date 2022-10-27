#ifndef __INDEXLIB_SORT_TRUNCATE_COLLECTOR_H
#define __INDEXLIB_SORT_TRUNCATE_COLLECTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_collector.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_map.h"
IE_NAMESPACE_BEGIN(index);

class SortTruncateCollector : public DocCollector
{
public:
    class Comparator 
    {
    public:
        Comparator(const BucketMapPtr& bucketMap)
        {
            mBucketMap = bucketMap;
        }

    public:
        bool operator() (docid_t left, docid_t right) 
        {
            return mBucketMap->GetSortValue(left) < mBucketMap->GetSortValue(right);
        }
    
    private:
        BucketMapPtr mBucketMap;
    } ;
public:
    SortTruncateCollector(uint64_t minDocCountToReserve, 
                          uint64_t maxDocCountToReserve,
                          const DocFilterProcessorPtr& filterProcessor,
                          const DocDistinctorPtr& docDistinctor,
                          const BucketVectorAllocatorPtr &bucketVecAllocator,
                          const BucketMapPtr &bucketMap,
                          int32_t memOptimizeThreshold);
    ~SortTruncateCollector();
public:
    void ReInit() override;
    void DoCollect(dictkey_t key, const PostingIteratorPtr& postingIt) override;
    void Truncate() override;
    void Reset() override
    {
        DocCollector::Reset();
        if (mBucketVector)
        {
            mBucketVecAllocator->DeallocateBucket(mBucketVector);
            mBucketVector.reset();
        }
    }
    int64_t EstimateMemoryUse(uint32_t docCount) const override;
private:
    static void SortBucket(DocIdVector& bucket, const BucketMapPtr &bucketMap);
    void SortLastValidBucketAndTruncate(uint64_t docCountToReserve);
    uint32_t AcquireDocCountToReserveWithDistinct();
private:
    BucketVectorPtr mBucketVector;
    BucketMapPtr mBucketMap;
    int32_t mMemOptimizeThreshold;
private:
    friend class SortTruncateCollectorTest;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SortTruncateCollector);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SORT_TRUNCATE_COLLECTOR_H
