#ifndef __INDEXLIB_NO_SORT_TRUNCATE_COLLECTOR_H
#define __INDEXLIB_NO_SORT_TRUNCATE_COLLECTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_collector.h"

IE_NAMESPACE_BEGIN(index);

class NoSortTruncateCollector : public DocCollector
{
public:
    NoSortTruncateCollector(uint64_t minDocCountToReserve, 
                            uint64_t maxDocCountToReserve, 
                            const DocFilterProcessorPtr& filterProcessor,
                            const DocDistinctorPtr& docDistinctor,
                            const BucketVectorAllocatorPtr &bucketVecAllocator);
    ~NoSortTruncateCollector();
public:
    void ReInit() override;
    void DoCollect(dictkey_t key, const PostingIteratorPtr& postingIt) override;
    void Truncate() override;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NoSortTruncateCollector);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_NO_SORT_TRUNCATE_COLLECTOR_H
