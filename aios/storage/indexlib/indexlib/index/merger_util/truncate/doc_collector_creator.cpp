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
#include "indexlib/index/merger_util/truncate/doc_collector_creator.h"

#include <time.h>

#include "indexlib/index/merger_util/truncate/multi_comparator.h"
#include "indexlib/index/merger_util/truncate/no_sort_truncate_collector.h"
#include "indexlib/index/merger_util/truncate/sort_truncate_collector.h"
#include "indexlib/util/class_typed_factory.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, DocCollectorCreator);

DocCollectorCreator::DocCollectorCreator() {}

DocCollectorCreator::~DocCollectorCreator() {}

DocCollectorPtr DocCollectorCreator::Create(const config::TruncateIndexProperty& truncateIndexProperty,
                                            const DocInfoAllocatorPtr& allocator, const BucketMaps& bucketMaps,
                                            TruncateAttributeReaderCreator* truncateAttrCreator,
                                            const BucketVectorAllocatorPtr& bucketVecAllocator,
                                            const TruncateMetaReaderPtr& metaReader, const EvaluatorPtr& evaluator)
{
    DocFilterProcessorPtr filter;
    if (truncateIndexProperty.HasFilter()) {
        filter = CreateFilter(truncateIndexProperty.mTruncateStrategy->GetDiversityConstrain(), truncateAttrCreator,
                              allocator, evaluator);
        if (truncateIndexProperty.IsFilterByMeta() || truncateIndexProperty.IsFilterByTimeStamp()) {
            filter->SetTruncateMetaReader(metaReader);
        }
    }

    DocDistinctorPtr docDistinct;
    DiversityConstrain constrain = truncateIndexProperty.mTruncateStrategy->GetDiversityConstrain();
    if (constrain.NeedDistinct()) {
        docDistinct = CreateDistinctor(constrain, allocator, truncateAttrCreator);
    }

    BucketMapPtr bucketMapPtr;
    BucketMaps::const_iterator it = bucketMaps.find(truncateIndexProperty.mTruncateProfile->mTruncateProfileName);
    if (it != bucketMaps.end()) {
        bucketMapPtr = it->second;
    }
    // TODO: docLimit refactor ???
    const auto& truncateStrategy = truncateIndexProperty.mTruncateStrategy;
    auto ret =
        DoCreateTruncatorCollector(truncateStrategy->GetLimit(), truncateStrategy->GetDiversityConstrain(),
                                   truncateStrategy->GetMemoryOptimizeThreshold(), docDistinct, filter, bucketMapPtr,
                                   bucketVecAllocator, truncateIndexProperty.mTruncateProfile, truncateAttrCreator);
    return DocCollectorPtr(ret);
}

std::unique_ptr<DocCollector>
DocCollectorCreator::CreateReTruncateCollector(const config::TruncateIndexProperty& truncateIndexProperty,
                                               const DocInfoAllocatorPtr& allocator, const BucketMaps& bucketMaps,
                                               TruncateAttributeReaderCreator* truncateAttrCreator,
                                               const BucketVectorAllocatorPtr& bucketVecAllocator,
                                               const TruncateMetaReaderPtr& metaReader, const EvaluatorPtr& evaluator)
{
    const auto& truncateStrategy = truncateIndexProperty.mTruncateStrategy;
    assert(truncateStrategy);
    const auto& reTruncateConfig = truncateStrategy->GetReTruncateConfig();

    if (!reTruncateConfig.NeedReTruncate()) {
        return nullptr;
    }
    const auto& constrain = reTruncateConfig.GetDiversityConstrain();
    DocFilterProcessorPtr filter;
    if (constrain.NeedFilter()) {
        filter = CreateFilter(constrain, truncateAttrCreator, allocator, evaluator);
    }

    DocDistinctorPtr docDistinct;
    if (constrain.NeedDistinct()) {
        docDistinct = CreateDistinctor(constrain, allocator, truncateAttrCreator);
    }

    BucketMapPtr bucketMapPtr;
    BucketMaps::const_iterator it = bucketMaps.find(truncateIndexProperty.mTruncateProfile->mTruncateProfileName);
    if (it != bucketMaps.end()) {
        bucketMapPtr = it->second;
    }
    // TODO: docLimit refactor ???
    auto ret =
        DoCreateTruncatorCollector(reTruncateConfig.GetLimit(), reTruncateConfig.GetDiversityConstrain(),
                                   truncateStrategy->GetMemoryOptimizeThreshold(), docDistinct, filter, bucketMapPtr,
                                   bucketVecAllocator, truncateIndexProperty.mTruncateProfile, truncateAttrCreator);
    return std::unique_ptr<DocCollector>(ret);
}

DocDistinctorPtr DocCollectorCreator::CreateDistinctor(const config::DiversityConstrain& constrain,
                                                       const DocInfoAllocatorPtr& allocator,
                                                       TruncateAttributeReaderCreator* truncateAttrCreator)
{
    // TODO: check distinct field type
    TruncateAttributeReaderPtr reader = truncateAttrCreator->Create(constrain.GetDistField());
    if (!reader) {
        return DocDistinctorPtr();
    }
    Reference* refer = allocator->GetReference(constrain.GetDistField());
    FieldType fieldType = refer->GetFieldType();
    DocDistinctor* dist =
        ClassTypedFactory<DocDistinctor, DocDistinctorTyped, TruncateAttributeReaderPtr, uint64_t>::GetInstance()
            ->Create(fieldType, reader, constrain.GetDistCount());
    return DocDistinctorPtr(dist);
}

DocFilterProcessorPtr DocCollectorCreator::CreateFilter(const config::DiversityConstrain& constrain,
                                                        TruncateAttributeReaderCreator* truncateAttrCreator,
                                                        const DocInfoAllocatorPtr& allocator,
                                                        const EvaluatorPtr& evaluator)
{
    const std::string filterField = constrain.GetFilterField();
    if (filterField == DOC_PAYLOAD_FIELD_NAME) {
        DocFilterProcessor* filter = new DocPayloadFilterProcessor(constrain, evaluator, allocator);
        return DocFilterProcessorPtr(filter);
    }
    TruncateAttributeReaderPtr reader = truncateAttrCreator->Create(filterField);
    if (!reader) {
        return DocFilterProcessorPtr();
    }
    Reference* refer = allocator->GetReference(filterField);
    assert(refer);
    FieldType fieldType = refer->GetFieldType();
    DocFilterProcessor* filter =
        ClassTypedFactory<DocFilterProcessor, DocFilterProcessorTyped, const TruncateAttributeReaderPtr,
                          const DiversityConstrain&>::GetInstance()
            ->Create(fieldType, reader, constrain);
    return DocFilterProcessorPtr(filter);
}

DocCollector* DocCollectorCreator::DoCreateTruncatorCollector(
    uint64_t minDocCountToReserve, const config::DiversityConstrain& constrain, int32_t memOptimizeThreshold,
    const DocDistinctorPtr& docDistinct, const DocFilterProcessorPtr& docFilter, const BucketMapPtr& bucketMap,
    const BucketVectorAllocatorPtr& bucketVecAllocator, const TruncateProfilePtr& truncateProfile,
    TruncateAttributeReaderCreator* truncateAttrCreator)
{
    uint64_t maxDocCountToReserve = GetMaxDocCountToReserve(minDocCountToReserve, constrain);

    if (!bucketMap && !truncateProfile->IsSortByDocPayload()) {
        return new NoSortTruncateCollector(minDocCountToReserve, maxDocCountToReserve, docFilter, docDistinct,
                                           bucketVecAllocator);
    } else {
        return new SortTruncateCollector(minDocCountToReserve, maxDocCountToReserve, docFilter, docDistinct,
                                         bucketVecAllocator, bucketMap, memOptimizeThreshold, truncateProfile,
                                         truncateAttrCreator);
    }
}

uint64_t DocCollectorCreator::GetMaxDocCountToReserve(uint64_t minDocCountToReserve,
                                                      const config::DiversityConstrain& constrain)
{
    if (constrain.NeedDistinct()) {
        uint64_t maxDocCountToReserve = constrain.GetDistExpandLimit();
        assert(maxDocCountToReserve >= minDocCountToReserve);
        return maxDocCountToReserve;
    }
    return minDocCountToReserve;
}
} // namespace indexlib::index::legacy
