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

#include "indexlib/common_define.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, IndexMetrics);
DECLARE_REFERENCE_CLASS(config, IndexConfig);

namespace indexlib { namespace index {
class SectionAttributeReader;
}} // namespace indexlib::index

namespace indexlib { namespace index { namespace legacy {
class MultiFieldIndexReader : public LegacyIndexReader
{
public:
    MultiFieldIndexReader(IndexMetrics* indexMetrics = NULL);
    ~MultiFieldIndexReader() {}

    MultiFieldIndexReader(const MultiFieldIndexReader&) = delete;
    MultiFieldIndexReader(MultiFieldIndexReader&&) = delete;

public:
    void AddIndexReader(const config::IndexConfigPtr& indexConf, const std::shared_ptr<InvertedIndexReader>& reader);

    // virtual for mock
    virtual const std::shared_ptr<InvertedIndexReader>& GetInvertedIndexReader(const std::string& field) const;

    const std::shared_ptr<InvertedIndexReader>& GetIndexReaderWithIndexId(indexid_t indexId) const;

    size_t GetIndexReaderCount() const { return mIndexReaders.size(); }

    index::Result<PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize = 1000,
                                           PostingType type = pt_default,
                                           autil::mem_pool::Pool* sessionPool = NULL) override;

    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* sessionPool,
                file_system::ReadOption option) noexcept override;

    index::Result<PostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                                  uint32_t statePoolSize, PostingType type,
                                                  autil::mem_pool::Pool* pool) override;

    const SectionAttributeReader* GetSectionReader(const std::string& indexName) const override;

    void SetLegacyAccessoryReader(const std::shared_ptr<LegacyIndexAccessoryReader>& accessoryReader) override;

    std::string GetIndexName() const;

    std::shared_ptr<KeyIterator> CreateKeyIterator(const std::string& indexName) override;

    bool GenFieldMapMask(const std::string& indexName, const std::vector<std::string>& termFieldNames,
                         fieldmap_t& targetFieldMap) override;

    void EnableAccessCountors() { mEnableAccessCountors = true; }

private:
    std::unordered_map<std::string, size_t> mName2PosMap;
    std::unordered_map<indexid_t, size_t> mIndexId2PosMap;
    std::vector<std::shared_ptr<InvertedIndexReader>> mIndexReaders;
    std::shared_ptr<LegacyIndexAccessoryReader> mAccessoryReader;
    mutable IndexMetrics* mIndexMetrics;
    mutable bool mEnableAccessCountors;

    static std::shared_ptr<InvertedIndexReader> RET_EMPTY_PTR;

private:
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<MultiFieldIndexReader> MultiFieldIndexReaderPtr;
}}} // namespace indexlib::index::legacy
