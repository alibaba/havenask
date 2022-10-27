#include "indexlib/index/normal/inverted_index/truncate/sort_truncate_collector.h"
#include "indexlib/util/math_util.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SortTruncateCollector);

SortTruncateCollector::SortTruncateCollector(uint64_t minDocCountToReserve, 
        uint64_t maxDocCountToReserve,
        const DocFilterProcessorPtr& filterProcessor,
        const DocDistinctorPtr& docDistinctor,
        const BucketVectorAllocatorPtr &bucketVecAllocator,
        const BucketMapPtr& bucketMap,
        int32_t memOptimizeThreshold) 
    : DocCollector(minDocCountToReserve, maxDocCountToReserve,
                   filterProcessor, docDistinctor, bucketVecAllocator)
    , mBucketMap(bucketMap)
    , mMemOptimizeThreshold(memOptimizeThreshold)
{
}

SortTruncateCollector::~SortTruncateCollector() 
{
}

void SortTruncateCollector::ReInit()
{
    mBucketVector = mBucketVecAllocator->AllocateBucket();
    assert(mBucketVector);
    uint32_t bucketCount = mBucketMap->GetBucketCount();
    uint32_t bucketSize = mBucketMap->GetBucketSize();
    uint32_t bucketReserveSize = MathUtil::MultiplyByPercentage(bucketSize, 
            mMemOptimizeThreshold / 2);
    mBucketVector->resize(bucketCount);
    for (uint32_t i = 0; i < bucketCount; ++i)
    {
        if ((*mBucketVector)[i].capacity() > bucketReserveSize)
        {
            DocIdVector emptyVec;
            (*mBucketVector)[i].swap(emptyVec);
            (*mBucketVector)[i].reserve(bucketReserveSize);
        }
        else
        {
            (*mBucketVector)[i].clear();
        }
    }
}

void SortTruncateCollector::DoCollect(dictkey_t key, const PostingIteratorPtr& postingIt)
{
    DocFilterProcessor *filter = mFilterProcessor.get();

    BucketMap *bucketMap = mBucketMap.get();
    BucketVector &bucketVec = (*mBucketVector);

    uint32_t validBucketCursor = bucketVec.size() - 1;
    uint32_t currentDocCount = 0;
    uint32_t maxDocCountToReserve = mMaxDocCountToReserve;

    docid_t docId = 0;
    while ((docId = postingIt->SeekDoc(docId)) != INVALID_DOCID)
    {
        if (filter && filter->IsFiltered(docId))
        {
            continue;
        }
        uint32_t bucketValue = bucketMap->GetBucketValue(docId);
        if (bucketValue > validBucketCursor)
        {
            continue;
        }
        bucketVec[bucketValue].push_back(docId);
        currentDocCount++;

        uint32_t lastValidBucketSize = bucketVec[validBucketCursor].size();
        if (currentDocCount > maxDocCountToReserve + lastValidBucketSize
            && validBucketCursor > 0)
        {
            validBucketCursor--;
            currentDocCount -= lastValidBucketSize;
        }
    }
}

void SortTruncateCollector::Truncate()
{
    uint32_t docCountToReserve = mMinDocCountToReserve;
    if (mDocDistinctor)
    {
        docCountToReserve = AcquireDocCountToReserveWithDistinct();
        if (docCountToReserve <= mMinDocCountToReserve)
        {
            docCountToReserve = mMinDocCountToReserve;
        } 
        else if (docCountToReserve >= mMaxDocCountToReserve)
        {
            docCountToReserve = mMaxDocCountToReserve;
        }
    }
    SortLastValidBucketAndTruncate(docCountToReserve);
}

void SortTruncateCollector::SortBucket(DocIdVector& bucket, const BucketMapPtr &bucketMap)
{
    Comparator comp(bucketMap);
    sort(bucket.begin(), bucket.end(), comp);
}

uint32_t SortTruncateCollector::AcquireDocCountToReserveWithDistinct()
{
    BucketVector &bucketVec = (*mBucketVector);
    uint32_t docCountToReserve = 0;
    DocDistinctor *docDistinct = mDocDistinctor.get();
    uint32_t maxDocCountToReserve = mMaxDocCountToReserve;
    uint32_t minDocCountToReserve = mMinDocCountToReserve;
    for (uint32_t i = 0; i < bucketVec.size(); ++i)
    {
        if (docCountToReserve + bucketVec[i].size() > minDocCountToReserve)
        {
            SortBucket(bucketVec[i], mBucketMap);
        }
        for (uint32_t j = 0; j < bucketVec[i].size(); ++j)
        {
            docCountToReserve++;
            if (docCountToReserve >= maxDocCountToReserve ||
                docDistinct->Distinct(bucketVec[i][j]))
            {
                return docCountToReserve;
            }
        }
    }
    return docCountToReserve;
}

void SortTruncateCollector::SortLastValidBucketAndTruncate(
        uint64_t docCountToReserve)
{
    BucketVector &bucketVec = (*mBucketVector);
    DocIdVector &docIdVec = (*mDocIdVec);
    uint32_t totalCount = 0;
    for (size_t i = 0; i < bucketVec.size(); ++i)
    {
        bool finish = false;
        uint32_t count = bucketVec[i].size();
        if (totalCount + bucketVec[i].size() >= docCountToReserve
            || i == bucketVec.size() - 1) // make sure last bucket ordered
        {
            SortBucket(bucketVec[i], mBucketMap);
            count = min(docCountToReserve - totalCount, bucketVec[i].size());
            finish = true;
        }
        if (count > 0)
        {
            docIdVec.insert(docIdVec.end(), bucketVec[i].begin(), bucketVec[i].begin() + count);
        }
        if (finish)
        {
            break;
        }
        totalCount += count;
    }
    if (docIdVec.empty())
    {
        return;
    }
    // depends on last bucket is ordered.
    mMinValueDocId = docIdVec.back();
    sort(docIdVec.begin(), docIdVec.end());
}

int64_t SortTruncateCollector::EstimateMemoryUse(uint32_t docCount) const
{
    int64_t size = DocCollector::EstimateMemoryUse(docCount);
    size += sizeof(docid_t) *docCount * 2; // mBucketVector
    return size;
}

IE_NAMESPACE_END(index);

