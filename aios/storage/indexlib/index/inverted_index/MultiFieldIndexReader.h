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
#include <string>
#include <unordered_map>

#include "autil/Log.h"
#include "future_lite/Executor.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/KeyIterator.h"

namespace indexlib::util {
class AccumulativeCounter;
}

namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlib::index {
class IndexAccessoryReader;
class IndexMetrics;

class MultiFieldIndexReader : public InvertedIndexReader
{
public:
    explicit MultiFieldIndexReader(
        const std::unordered_map<std::string, std::shared_ptr<indexlib::util::AccumulativeCounter>>*
            invertedAccessCounter);
    ~MultiFieldIndexReader() = default;
    MultiFieldIndexReader(const MultiFieldIndexReader&) = delete;
    MultiFieldIndexReader(MultiFieldIndexReader&&) = delete;

public:
    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const indexlibv2::framework::TabletData* tabletData) override
    {
        assert(false);
        return Status::Unimplement("never got hear");
    }

    virtual const std::shared_ptr<InvertedIndexReader>& GetIndexReader(const std::string& field) const;

    index::Result<PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize, PostingType type,
                                           autil::mem_pool::Pool* sessionPool) override;

    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* sessionPool,
                file_system::ReadOption option) noexcept override;

    index::Result<PostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                                  uint32_t statePoolSize, PostingType type,
                                                  autil::mem_pool::Pool* pool) override;

    const SectionAttributeReader* GetSectionReader(const std::string& indexName) const override;

    void SetAccessoryReader(const std::shared_ptr<IndexAccessoryReader>& accessoryReader) override;

    std::shared_ptr<KeyIterator> CreateKeyIterator(const std::string& indexName) override;

    bool GenFieldMapMask(const std::string& indexName, const std::vector<std::string>& termFieldNames,
                         fieldmap_t& targetFieldMap) override;

    Status AddIndexReader(const indexlibv2::config::InvertedIndexConfig* indexConfig,
                          const std::shared_ptr<InvertedIndexReader>& reader);
    const std::shared_ptr<InvertedIndexReader>& GetIndexReaderWithIndexId(indexid_t indexId) const;
    size_t GetIndexReaderCount() const { return _indexReaders.size(); }

private:
    void IncAccessCounter(const std::string& indexName) const;

private:
    bool GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           file_system::ReadOption option, InvertedIndexSearchTracer* tracer) override
    {
        assert(false);
        return false;
    }
    future_lite::coro::Lazy<index::Result<bool>>
    GetSegmentPostingAsync(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           file_system::ReadOption option, InvertedIndexSearchTracer* tracer) noexcept override
    {
        assert(false);
        co_return false;
    }

private:
    std::unordered_map<std::string, size_t> _name2PosMap;
    std::unordered_map<indexid_t, size_t> _indexId2PosMap;
    std::vector<std::shared_ptr<InvertedIndexReader>> _indexReaders;
    std::shared_ptr<IndexAccessoryReader> _accessoryReader;
    const std::unordered_map<std::string, std::shared_ptr<indexlib::util::AccumulativeCounter>>* _accessCounter =
        nullptr;

    static std::shared_ptr<InvertedIndexReader> RET_EMPTY_PTR;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
