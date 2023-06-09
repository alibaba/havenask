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

#include "indexlib/index/inverted_index/truncate/DocCollector.h"

namespace indexlib::index {

class NoSortTruncateCollector : public DocCollector
{
public:
    NoSortTruncateCollector(uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                            const std::shared_ptr<IDocFilterProcessor>& filterProcessor,
                            const std::shared_ptr<DocDistinctor>& docDistinctor,
                            const std::shared_ptr<BucketVectorAllocator>& bucketVecAllocator);
    ~NoSortTruncateCollector() = default;

public:
    void ReInit() override;
    void DoCollect(const std::shared_ptr<PostingIterator>& postingIt) override;
    void Truncate() override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
