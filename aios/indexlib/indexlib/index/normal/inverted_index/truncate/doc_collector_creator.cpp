#include <time.h>
#include "indexlib/index/normal/inverted_index/truncate/doc_collector_creator.h"
#include "indexlib/util/class_typed_factory.h"
#include "indexlib/index/normal/inverted_index/truncate/multi_comparator.h"
#include "indexlib/index/normal/inverted_index/truncate/sort_truncate_collector.h"
#include "indexlib/index/normal/inverted_index/truncate/no_sort_truncate_collector.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DocCollectorCreator);

DocCollectorCreator::DocCollectorCreator() 
{
}

DocCollectorCreator::~DocCollectorCreator() 
{
}

DocCollectorPtr DocCollectorCreator::Create(
        const config::TruncateIndexProperty& truncateIndexProperty,
        const DocInfoAllocatorPtr& allocator,
        const BucketMaps& bucketMaps,
        TruncateAttributeReaderCreator *truncateAttrCreator,
        const BucketVectorAllocatorPtr &bucketVecAllocator,
        const TruncateMetaReaderPtr& metaReader)
{
    DocFilterProcessorPtr filter;
    if (truncateIndexProperty.HasFilter())
    {
        filter = CreateFilter(truncateIndexProperty, truncateAttrCreator, allocator);
        if (truncateIndexProperty.IsFilterByMeta()
            || truncateIndexProperty.IsFilterByTimeStamp())
        {
            filter->SetTruncateMetaReader(metaReader);
        }
    }

    DocDistinctorPtr docDistinct;
    DiversityConstrain constrain = 
        truncateIndexProperty.mTruncateStrategy->GetDiversityConstrain();
    if (constrain.NeedDistinct())
    {
        docDistinct = CreateDistinctor(truncateIndexProperty,
                allocator, truncateAttrCreator);
    }

    BucketMapPtr bucketMapPtr;
    BucketMaps::const_iterator it = bucketMaps.find(
            truncateIndexProperty.mTruncateProfile->mTruncateProfileName);
    if (it != bucketMaps.end())
    {
        bucketMapPtr = it->second;
    }
    //TODO: docLimit refactor ???
    return DoCreateTruncatorCollector(truncateIndexProperty.mTruncateStrategy,
            docDistinct, filter, bucketMapPtr, bucketVecAllocator);
}

DocDistinctorPtr DocCollectorCreator::CreateDistinctor(
        const TruncateIndexProperty& truncateIndexProperty,
        const DocInfoAllocatorPtr& allocator,
        TruncateAttributeReaderCreator *truncateAttrCreator)
{
    //TODO: check distinct field type
    const DiversityConstrain& constrain = 
        truncateIndexProperty.mTruncateStrategy->GetDiversityConstrain();
    TruncateAttributeReaderPtr reader = truncateAttrCreator->Create(constrain.GetDistField());
    if (!reader)
    {
        return DocDistinctorPtr();
    }
    Reference* refer = allocator->GetReference(constrain.GetDistField());
    FieldType fieldType = refer->GetFieldType();
    DocDistinctor *dist = 
        ClassTypedFactory<DocDistinctor, DocDistinctorTyped, 
                          TruncateAttributeReaderPtr, uint64_t>::GetInstance()->Create(
                                  fieldType, reader, constrain.GetDistCount());
    return DocDistinctorPtr(dist);
}

DocFilterProcessorPtr DocCollectorCreator::CreateFilter(
        const TruncateIndexProperty& truncateIndexProperty,
        TruncateAttributeReaderCreator *truncateAttrCreator,
        const DocInfoAllocatorPtr& allocator)
{
    const DiversityConstrain& constrain = 
        truncateIndexProperty.mTruncateStrategy->GetDiversityConstrain();
    const std::string filterField = constrain.GetFilterField();
    if (filterField == DOC_PAYLOAD_FIELD_NAME)
    {
        DocFilterProcessor* filter = new DocPayloadFilterProcessor(constrain);
        return DocFilterProcessorPtr(filter);
    }
    TruncateAttributeReaderPtr reader = truncateAttrCreator->Create(filterField);
    if (!reader)
    {
        return DocFilterProcessorPtr();
    }
    Reference* refer = allocator->GetReference(filterField);
    assert(refer);
    FieldType fieldType = refer->GetFieldType();
    DocFilterProcessor* filter = 
        ClassTypedFactory<DocFilterProcessor, DocFilterProcessorTyped, 
                          const TruncateAttributeReaderPtr,
                          const DiversityConstrain&
                          >::GetInstance()->Create(fieldType, reader, constrain);
    return DocFilterProcessorPtr(filter);
}

DocCollectorPtr DocCollectorCreator::DoCreateTruncatorCollector(
        const TruncateStrategyPtr& truncateStrategy,
        const DocDistinctorPtr& docDistinct, 
        const DocFilterProcessorPtr &docFilter,
        const BucketMapPtr& bucketMap,
        const BucketVectorAllocatorPtr &bucketVecAllocator)
{
    uint64_t minDocCountToReserve = truncateStrategy->GetLimit();
    uint64_t maxDocCountToReserve = GetMaxDocCountToReserve(truncateStrategy);
    int32_t memOptimizeThreshold = truncateStrategy->GetMemoryOptimizeThreshold();

    if (!bucketMap)
    {
        return DocCollectorPtr(new NoSortTruncateCollector(
                        minDocCountToReserve, maxDocCountToReserve, docFilter,
                        docDistinct, bucketVecAllocator));
    }
    else
    {
        return DocCollectorPtr(new SortTruncateCollector(
                        minDocCountToReserve, maxDocCountToReserve, docFilter,
                        docDistinct, bucketVecAllocator, bucketMap,
                        memOptimizeThreshold));
    }
}

uint64_t DocCollectorCreator::GetMaxDocCountToReserve(
        const TruncateStrategyPtr& truncateStrategy)
{
    uint64_t  minDocCountToReserve = truncateStrategy->GetLimit();
    const DiversityConstrain& constrain = truncateStrategy->GetDiversityConstrain();
    if (constrain.NeedDistinct())
    {
        uint64_t maxDocCountToReserve = constrain.GetDistExpandLimit();
        assert(maxDocCountToReserve >= minDocCountToReserve);
        return maxDocCountToReserve;
    }
    return minDocCountToReserve;
}

IE_NAMESPACE_END(index);

