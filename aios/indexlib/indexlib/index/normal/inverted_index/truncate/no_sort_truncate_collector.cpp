#include "indexlib/index/normal/inverted_index/truncate/no_sort_truncate_collector.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, NoSortTruncateCollector);

NoSortTruncateCollector::NoSortTruncateCollector(uint64_t minDocCountToReserve, 
        uint64_t maxDocCountToReserve, 
        const DocFilterProcessorPtr& filterProcessor,
        const DocDistinctorPtr& docDistinctor,
        const BucketVectorAllocatorPtr &bucketVecAllocator) 
    : DocCollector(minDocCountToReserve, maxDocCountToReserve,
                   filterProcessor, docDistinctor, bucketVecAllocator)
{
}

NoSortTruncateCollector::~NoSortTruncateCollector() 
{
}

void NoSortTruncateCollector::ReInit()
{
}

void NoSortTruncateCollector::DoCollect(
        dictkey_t key, const PostingIteratorPtr& postingIt)
{
    DocFilterProcessor *filter = mFilterProcessor.get();
    DocDistinctor *docDistinctor = mDocDistinctor.get();
    DocIdVector &docIdVec = *mDocIdVec;
    uint32_t maxDocCountToReserve = mMaxDocCountToReserve;
    uint32_t minDocCountToReserve = mMinDocCountToReserve;
    docid_t docId = 0;
    while ((docId = postingIt->SeekDoc(docId)) != INVALID_DOCID)
    {
        if (filter && filter->IsFiltered(docId))
        {
            continue;
        }
        if (!docDistinctor)
        {
            if (docIdVec.size() < minDocCountToReserve)
            {
                docIdVec.push_back(docId);
            }
            else
            {
                return;
            }
        }
        else
        {
            if (docIdVec.size() >= maxDocCountToReserve)
            {
                return;
            }
            else if (!docDistinctor->IsFull())
            {
                docDistinctor->Distinct(docId);
                docIdVec.push_back(docId);
            }
            else if (docIdVec.size() < minDocCountToReserve)
            {
                docIdVec.push_back(docId);                
            }
            else
            {
                return;
            }
        }
    }
}

void NoSortTruncateCollector::Truncate()
{
    if (mDocIdVec->size() > 0)
    {
        mMinValueDocId = mDocIdVec->back(); // useless, set it for case.
    }
}

IE_NAMESPACE_END(index);

