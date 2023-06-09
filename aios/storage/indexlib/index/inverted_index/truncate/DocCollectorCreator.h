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
#pragma once
#include <memory>

#include "indexlib/index/inverted_index/truncate/BucketMapCreator.h"
#include "indexlib/index/inverted_index/truncate/Comparator.h"
#include "indexlib/index/inverted_index/truncate/DocCollector.h"
#include "indexlib/index/inverted_index/truncate/DocDistinctor.h"
#include "indexlib/index/inverted_index/truncate/DocFilterProcessorTyped.h"
#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"
#include "indexlib/index/inverted_index/truncate/DocPayloadFilterProcessor.h"
#include "indexlib/index/inverted_index/truncate/IDocFilterProcessor.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReaderCreator.h"
#include "indexlib/index/inverted_index/truncate/TruncateMetaReader.h"

namespace indexlib::index {

class DocCollectorCreator
{
public:
    DocCollectorCreator() = default;
    ~DocCollectorCreator() = default;

public:
    static std::unique_ptr<DocCollector>
    Create(const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty,
           const std::shared_ptr<DocInfoAllocator>& allocator, const BucketMaps& bucketMaps,
           const std::shared_ptr<TruncateAttributeReaderCreator>& truncateAttrCreator,
           const std::shared_ptr<BucketVectorAllocator>& bucketVecAllocator,
           const std::shared_ptr<TruncateMetaReader>& metaReader, const std::shared_ptr<IEvaluator>& evaluator);

private:
    static std::shared_ptr<DocDistinctor>
    CreateDistinctor(const indexlibv2::config::DiversityConstrain& constrain,
                     const std::shared_ptr<TruncateAttributeReaderCreator>& truncateAttrCreator,
                     const std::shared_ptr<DocInfoAllocator>& allocatory);

    static std::shared_ptr<IDocFilterProcessor>
    CreateFilter(const indexlibv2::config::DiversityConstrain& constrain,
                 const std::shared_ptr<TruncateAttributeReaderCreator>& truncateAttrCreator,
                 const std::shared_ptr<DocInfoAllocator>& allocator, const std::shared_ptr<IEvaluator>& evaluator);

    static std::unique_ptr<DocCollector> DoCreateTruncatorCollector(
        const std::shared_ptr<indexlibv2::config::TruncateStrategy>& truncateStrategy,
        const std::shared_ptr<DocDistinctor>& docDistinct, const std::shared_ptr<IDocFilterProcessor>& docFilter,
        const std::shared_ptr<BucketMap>& bucketMap, const std::shared_ptr<BucketVectorAllocator>& bucketVecAllocator,
        const std::shared_ptr<indexlibv2::config::TruncateProfile>& truncateProfile,
        TruncateAttributeReaderCreator* truncateAttrCreator);

    static uint64_t
    GetMaxDocCountToReserve(const std::shared_ptr<indexlibv2::config::TruncateStrategy>& truncateStrategy);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
