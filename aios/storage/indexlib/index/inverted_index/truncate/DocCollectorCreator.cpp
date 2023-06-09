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
#include "indexlib/index/inverted_index/truncate/DocCollectorCreator.h"

#include <time.h>

#include "indexlib/index/inverted_index/truncate/MultiComparator.h"
#include "indexlib/index/inverted_index/truncate/NoSortTruncateCollector.h"
#include "indexlib/index/inverted_index/truncate/SortTruncateCollector.h"
#include "indexlib/util/ClassTypedFactory.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, DocCollectorCreator);

std::unique_ptr<DocCollector>
DocCollectorCreator::Create(const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty,
                            const std::shared_ptr<DocInfoAllocator>& allocator, const BucketMaps& bucketMaps,
                            const std::shared_ptr<TruncateAttributeReaderCreator>& truncateAttrCreator,
                            const std::shared_ptr<BucketVectorAllocator>& bucketVecAllocator,
                            const std::shared_ptr<TruncateMetaReader>& metaReader,
                            const std::shared_ptr<IEvaluator>& evaluator)
{
    const auto& truncateStrategy = truncateIndexProperty.truncateStrategy;
    std::shared_ptr<DocDistinctor> docDistinct;
    indexlibv2::config::DiversityConstrain constrain = truncateIndexProperty.truncateStrategy->GetDiversityConstrain();
    if (constrain.NeedDistinct()) {
        docDistinct = CreateDistinctor(constrain, truncateAttrCreator, allocator);
    }

    std::shared_ptr<IDocFilterProcessor> filter;
    if (truncateIndexProperty.HasFilter()) {
        filter = CreateFilter(truncateIndexProperty.truncateStrategy->GetDiversityConstrain(), truncateAttrCreator,
                              allocator, evaluator);
        if (truncateIndexProperty.IsFilterByMeta() || truncateIndexProperty.IsFilterByTimeStamp()) {
            filter->SetTruncateMetaReader(metaReader);
        }
    }

    std::shared_ptr<BucketMap> bucketMap;
    BucketMaps::const_iterator it = bucketMaps.find(truncateIndexProperty.truncateProfile->truncateProfileName);
    if (it != bucketMaps.end()) {
        bucketMap = it->second;
    }
    return DoCreateTruncatorCollector(truncateStrategy, docDistinct, filter, bucketMap, bucketVecAllocator,
                                      truncateIndexProperty.truncateProfile, truncateAttrCreator.get());
}

std::shared_ptr<DocDistinctor>
DocCollectorCreator::CreateDistinctor(const indexlibv2::config::DiversityConstrain& constrain,
                                      const std::shared_ptr<TruncateAttributeReaderCreator>& truncateAttrCreator,
                                      const std::shared_ptr<DocInfoAllocator>& allocator)
{
    auto reader = truncateAttrCreator->Create(constrain.GetDistField());
    if (!reader) {
        return nullptr;
    }

    Reference* refer = allocator->GetReference(constrain.GetDistField());
    FieldType fieldType = refer->GetFieldType();
    auto dist = indexlib::util::ClassTypedFactory<DocDistinctor, DocDistinctorTyped,
                                                  std::shared_ptr<TruncateAttributeReader>, uint64_t>::GetInstance()
                    ->Create(fieldType, reader, constrain.GetDistCount());
    return std::shared_ptr<DocDistinctor>(dist);
}

std::shared_ptr<IDocFilterProcessor>
DocCollectorCreator::CreateFilter(const indexlibv2::config::DiversityConstrain& constrain,
                                  const std::shared_ptr<TruncateAttributeReaderCreator>& truncateAttrCreator,
                                  const std::shared_ptr<DocInfoAllocator>& allocator,
                                  const std::shared_ptr<IEvaluator>& evaluator)
{
    auto filterField = constrain.GetFilterField();
    if (filterField == DOC_PAYLOAD_FIELD_NAME) {
        return std::make_shared<DocPayloadFilterProcessor>(constrain, evaluator, allocator);
    }

    auto reader = truncateAttrCreator->Create(filterField);
    if (!reader) {
        return nullptr;
    }
    Reference* refer = allocator->GetReference(filterField);
    assert(refer);
    FieldType fieldType = refer->GetFieldType();
    auto filter = indexlib::util::ClassTypedFactory<IDocFilterProcessor, DocFilterProcessorTyped,
                                                    const std::shared_ptr<TruncateAttributeReader>,
                                                    const indexlibv2::config::DiversityConstrain&>::GetInstance()
                      ->Create(fieldType, reader, constrain);
    return std::shared_ptr<IDocFilterProcessor>(filter);
}

std::unique_ptr<DocCollector> DocCollectorCreator::DoCreateTruncatorCollector(
    const std::shared_ptr<indexlibv2::config::TruncateStrategy>& truncateStrategy,
    const std::shared_ptr<DocDistinctor>& docDistinct, const std::shared_ptr<IDocFilterProcessor>& docFilter,
    const std::shared_ptr<BucketMap>& bucketMap, const std::shared_ptr<BucketVectorAllocator>& bucketVecAllocator,
    const std::shared_ptr<indexlibv2::config::TruncateProfile>& truncateProfile,
    TruncateAttributeReaderCreator* truncateAttrCreator)
{
    uint64_t minDocCountToReserve = truncateStrategy->GetLimit();
    uint64_t maxDocCountToReserve = GetMaxDocCountToReserve(truncateStrategy);
    int32_t memOptimizeThreshold = truncateStrategy->GetMemoryOptimizeThreshold();

    if (!bucketMap && !truncateProfile->IsSortByDocPayload()) {
        return std::make_unique<NoSortTruncateCollector>(minDocCountToReserve, maxDocCountToReserve, docFilter,
                                                         docDistinct, bucketVecAllocator);
    } else {
        return std::make_unique<SortTruncateCollector>(minDocCountToReserve, maxDocCountToReserve, docFilter,
                                                       docDistinct, bucketVecAllocator, bucketMap, memOptimizeThreshold,
                                                       truncateProfile, truncateAttrCreator);
    }
}

uint64_t DocCollectorCreator::GetMaxDocCountToReserve(
    const std::shared_ptr<indexlibv2::config::TruncateStrategy>& truncateStrategy)
{
    auto minDocCountToReserve = truncateStrategy->GetLimit();
    auto constrain = truncateStrategy->GetDiversityConstrain();
    if (constrain.NeedDistinct()) {
        uint64_t maxDocCountToReserve = constrain.GetDistExpandLimit();
        assert(maxDocCountToReserve >= minDocCountToReserve);
        return maxDocCountToReserve;
    }
    return minDocCountToReserve;
}

} // namespace indexlib::index
