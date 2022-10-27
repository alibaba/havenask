#ifndef __INDEXLIB_DOC_COLLECTOR_CREATOR_H
#define __INDEXLIB_DOC_COLLECTOR_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_collector.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_common.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"
#include "indexlib/index/normal/inverted_index/truncate/comparator.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_filter_processor.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_payload_filter_processor.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_meta_reader.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_map_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_distinctor.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_attribute_reader_creator.h"

IE_NAMESPACE_BEGIN(index);

class DocCollectorCreator
{
public:
    DocCollectorCreator();
    ~DocCollectorCreator();

public:
    static DocCollectorPtr Create(
            const config::TruncateIndexProperty& truncateIndexProperty,
            const DocInfoAllocatorPtr& allocator,
            const BucketMaps& bucketMaps,
            TruncateAttributeReaderCreator *truncateAttrCreator,
            const BucketVectorAllocatorPtr &bucketVecAllocator,
            const TruncateMetaReaderPtr& metaReader = TruncateMetaReaderPtr());

private:
    static DocDistinctorPtr CreateDistinctor(
            const config::TruncateIndexProperty& truncateIndexProperty,
            const DocInfoAllocatorPtr& allocatory,
            TruncateAttributeReaderCreator *truncateAttrCreator);

    static DocFilterProcessorPtr CreateFilter(
            const config::TruncateIndexProperty& truncateIndexProperty,
            TruncateAttributeReaderCreator *truncateAttrCreator,
            const DocInfoAllocatorPtr& allocator);

    static DocCollectorPtr DoCreateTruncatorCollector(
            const config::TruncateStrategyPtr& truncateStrategy,
            const DocDistinctorPtr& docDistinct,
            const DocFilterProcessorPtr &docFilter,
            const BucketMapPtr& bucketMap,
            const BucketVectorAllocatorPtr &bucketVecAllocator);

    static uint64_t GetMaxDocCountToReserve(
            const config::TruncateStrategyPtr& truncateStrategy);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocCollectorCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_COLLECTOR_CREATOR_H
