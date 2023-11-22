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

#include "autil/HashAlgorithm.h"
#include "fslib/fslib.h"
#include "indexlib/index/common/DictHasher.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicIndexSegmentReader.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicIndexTermUpdater.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicMemIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicPostingIterator.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"
namespace indexlibv2 { namespace framework {
class TabletData;
}} // namespace indexlibv2::framework
namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlib::index {
class SegmentPosting;
class BuildingIndexReader;

class DynamicIndexReader
{
public:
    DynamicIndexReader();
    DynamicIndexReader(const DynamicIndexReader& other) = delete;
    DynamicIndexReader& operator=(const DynamicIndexReader& other) = delete;
    virtual ~DynamicIndexReader();

public:
    void
    Open(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
         std::vector<std::tuple</*baseDocId=*/docid64_t, /*segmentDocCount=*/uint64_t,
                                /*dynamicPostingResourceFile=*/std::shared_ptr<indexlib::file_system::ResourceFile>>>
             dynamicPostingResources,
         std::vector<std::pair<docid64_t, std::shared_ptr<DynamicIndexSegmentReader>>> dynamicSegmentReaders);

    // default poolSize 1000, sessionPool null
    index::Result<DynamicPostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize,
                                                  autil::mem_pool::Pool* sessionPool) const;

    index::Result<DynamicPostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                                         uint32_t statePoolSize,
                                                         autil::mem_pool::Pool* sessionPool) const;

public:
    void TEST_SetIndexConfig(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig);

private:
    void
    LoadSegments(std::vector<std::tuple<docid64_t, uint64_t, std::shared_ptr<indexlib::file_system::ResourceFile>>>
                     dynamicPostingResources,
                 std::vector<std::pair<docid64_t, std::shared_ptr<DynamicIndexSegmentReader>>> dynamicSegmentReaders);
    DynamicPostingIterator* Lookup(const index::DictKeyInfo& key, const DocIdRangeVector& docIdRanges,
                                   uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool) const;
    void AddInMemSegmentReader(docid64_t baseDocId, const std::shared_ptr<DynamicIndexSegmentReader>& inMemSegReader);
    void FillSegmentContexts(const index::DictKeyInfo& key, autil::mem_pool::Pool* sessionPool,
                             std::vector<DynamicPostingIterator::SegmentContext>& segmentContexts) const;
    void FillSegmentContextsByRanges(const index::DictKeyInfo& key, const DocIdRangeVector& ranges,
                                     autil::mem_pool::Pool* sessionPool,
                                     std::vector<DynamicPostingIterator::SegmentContext>& segmentContexts) const;

private:
    IndexDictHasher _dictHasher;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    IndexFormatOption _indexFormatOption;
    // built segment
    std::vector<std::shared_ptr<indexlib::file_system::ResourceFile>> _postingResources;
    std::vector<uint64_t> _segmentDocCount;
    std::vector<docid64_t> _baseDocIds;
    // building segment
    std::shared_ptr<BuildingIndexReader> _buildingIndexReader;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
