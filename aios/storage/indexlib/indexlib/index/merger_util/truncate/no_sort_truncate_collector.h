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
#ifndef __INDEXLIB_NO_SORT_TRUNCATE_COLLECTOR_H
#define __INDEXLIB_NO_SORT_TRUNCATE_COLLECTOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/doc_collector.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class NoSortTruncateCollector : public DocCollector
{
public:
    NoSortTruncateCollector(uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                            const DocFilterProcessorPtr& filterProcessor, const DocDistinctorPtr& docDistinctor,
                            const BucketVectorAllocatorPtr& bucketVecAllocator);
    ~NoSortTruncateCollector();

public:
    void ReInit() override;
    void DoCollect(const index::DictKeyInfo& key, const std::shared_ptr<PostingIterator>& postingIt) override;
    void Truncate() override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NoSortTruncateCollector);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_NO_SORT_TRUNCATE_COLLECTOR_H
