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

#include "indexlib/index/IDiskIndexer.h"

namespace indexlib::document {
class ModifiedTokens;
}

namespace indexlib::index {
class IndexUpdateTermIterator;

class IInvertedDiskIndexer : public indexlibv2::index::IDiskIndexer
{
public:
    IInvertedDiskIndexer() = default;
    virtual ~IInvertedDiskIndexer() = default;

    virtual void UpdateTokens(docid_t localDocId, const document::ModifiedTokens& modifiedTokens) = 0;
    virtual void UpdateTerms(IndexUpdateTermIterator* termIter) = 0;
    virtual void UpdateBuildResourceMetrics(size_t& poolMemory, size_t& totalMemory, size_t& totalRetiredMemory,
                                            size_t& totalDocCount, size_t& totalAlloc, size_t& totalFree,
                                            size_t& totalTreeCount) const = 0;
    virtual std::shared_ptr<IDiskIndexer> GetSectionAttributeDiskIndexer() const = 0;
};

} // namespace indexlib::index
