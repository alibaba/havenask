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
#ifndef __INDEXLIB_DOC_COLLECTOR_CREATOR_H
#define __INDEXLIB_DOC_COLLECTOR_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/truncate_profile.h"
#include "indexlib/index/merger_util/truncate/bucket_map_creator.h"
#include "indexlib/index/merger_util/truncate/comparator.h"
#include "indexlib/index/merger_util/truncate/doc_collector.h"
#include "indexlib/index/merger_util/truncate/doc_distinctor.h"
#include "indexlib/index/merger_util/truncate/doc_filter_processor.h"
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"
#include "indexlib/index/merger_util/truncate/doc_payload_filter_processor.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader_creator.h"
#include "indexlib/index/merger_util/truncate/truncate_common.h"
#include "indexlib/index/merger_util/truncate/truncate_meta_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class DocCollectorCreator
{
public:
    DocCollectorCreator();
    ~DocCollectorCreator();

public:
    static DocCollectorPtr Create(const config::TruncateIndexProperty& truncateIndexProperty,
                                  const DocInfoAllocatorPtr& allocator, const BucketMaps& bucketMaps,
                                  TruncateAttributeReaderCreator* truncateAttrCreator,
                                  const BucketVectorAllocatorPtr& bucketVecAllocator,
                                  const TruncateMetaReaderPtr& metaReader = TruncateMetaReaderPtr(),
                                  const EvaluatorPtr& evaluator = EvaluatorPtr());

    static std::unique_ptr<DocCollector>
    CreateReTruncateCollector(const config::TruncateIndexProperty& truncateIndexProperty,
                              const DocInfoAllocatorPtr& allocator, const BucketMaps& bucketMaps,
                              TruncateAttributeReaderCreator* truncateAttrCreator,
                              const BucketVectorAllocatorPtr& bucketVecAllocator,
                              const TruncateMetaReaderPtr& metaReader = TruncateMetaReaderPtr(),
                              const EvaluatorPtr& evaluator = EvaluatorPtr());

private:
    static DocDistinctorPtr CreateDistinctor(const config::DiversityConstrain& constrain,
                                             const DocInfoAllocatorPtr& allocatory,
                                             TruncateAttributeReaderCreator* truncateAttrCreator);

    static DocFilterProcessorPtr CreateFilter(const config::DiversityConstrain& constrain,
                                              TruncateAttributeReaderCreator* truncateAttrCreator,
                                              const DocInfoAllocatorPtr& allocator, const EvaluatorPtr& evaluator);

    static DocCollector* DoCreateTruncatorCollector(
        uint64_t minDocCountToReserve, const config::DiversityConstrain& constrain, int32_t memOptimizeThreshold,
        const DocDistinctorPtr& docDistinct, const DocFilterProcessorPtr& docFilter, const BucketMapPtr& bucketMap,
        const BucketVectorAllocatorPtr& bucketVecAllocator, const config::TruncateProfilePtr& truncateProfile,
        TruncateAttributeReaderCreator* truncateAttrCreator);

    static uint64_t GetMaxDocCountToReserve(uint64_t minDocCountToReserve, const config::DiversityConstrain& constrain);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocCollectorCreator);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_DOC_COLLECTOR_CREATOR_H
