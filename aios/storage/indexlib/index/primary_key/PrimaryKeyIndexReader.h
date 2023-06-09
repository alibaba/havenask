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

#include "autil/ConstString.h"
#include "autil/MultiValueType.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/primary_key/Types.h"

namespace future_lite {
class Executor;
}

namespace indexlibv2::config {
class IIndexConfig;
}
namespace indexlibv2::index {
class AttributeReader;
}

namespace indexlib { namespace index {

class AttributeReader;
class SectionAttributeReader;

// depends on InvertedIndexReader
class PrimaryKeyIndexReader : public InvertedIndexReader
{
private:
    using InvertedIndexReader::Lookup;

public:
    PrimaryKeyIndexReader() : _isNumberHash(false), _is128Pk(false) {}
    virtual ~PrimaryKeyIndexReader() {}

public:
    virtual Status OpenWithoutPKAttribute(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                          const indexlibv2::framework::TabletData* tabletData) = 0;

    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* pool,
                file_system::ReadOption option) noexcept override
    {
        co_return Lookup(*term, statePoolSize, type, pool);
    }

    virtual docid_t Lookup(const std::string& pkStr, future_lite::Executor* executor) const = 0;
    virtual docid_t Lookup(const std::string& pkStr) const { return Lookup(pkStr, nullptr); }
    virtual docid_t Lookup(const autil::StringView& pkStr) const { return INVALID_DOCID; }
    virtual docid_t LookupWithPKHash(const autil::uint128_t& pkHash,
                                     future_lite::Executor* executor = nullptr) const = 0;
    virtual bool LookupWithPKHash(const autil::uint128_t& pkHash, segmentid_t specifySegment, docid_t* docid) const = 0;

    virtual std::shared_ptr<indexlibv2::index::AttributeReader> GetPKAttributeReader() const = 0;

    virtual docid_t LookupWithDocRange(const autil::uint128_t& pkHash, std::pair<docid_t, docid_t> docRange,
                                       future_lite::Executor* executor) const
    {
        assert(false);
        return INVALID_DOCID;
    }
    virtual docid_t LookupWithHintValues(const autil::uint128_t& pkHash, int32_t hintValues) const
    {
        assert(false);
        return INVALID_DOCID;
    }
    virtual future_lite::Executor* GetBuildExecutor() const { return nullptr; }

    virtual bool CheckDuplication() const { return true; }

    virtual size_t EvaluateCurrentMemUsed() { return 0; }

public:
    // for index_printer, pair<docid, isDeleted>
    virtual bool LookupAll(const std::string& pkStr, std::vector<std::pair<docid_t, bool>>& docidPairVec) const
    {
        return false;
    }

public:
    template <typename T>
    inline docid_t LookupWithType(const T& key, future_lite::Executor* executor = nullptr) const;

public:
    bool Is128PK() const { return _is128Pk; }

private:
    std::shared_ptr<KeyIterator> CreateKeyIterator(const std::string& indexName) override
    {
        return std::shared_ptr<KeyIterator>();
    }

    const SectionAttributeReader* GetSectionReader(const std::string& indexName) const override { return NULL; }

    void SetAccessoryReader(const std::shared_ptr<IndexAccessoryReader>& accessorReader) override {}

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

protected:
    // _isNumberHash and _is128Pk should be rewrite by child classes
    bool _isNumberHash;
    bool _is128Pk;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
docid_t PrimaryKeyIndexReader::LookupWithType(const T& key, future_lite::Executor* executor) const
{
    if (_isNumberHash && !_is128Pk) {
        const autil::uint128_t pkHash((uint64_t)key);
        return LookupWithPKHash(pkHash, executor);
    }
    const std::string& strKey = autil::StringUtil::toString(key);
    return Lookup(strKey, executor);
}

template <>
inline docid_t PrimaryKeyIndexReader::LookupWithType(const std::string& key, future_lite::Executor* executor) const
{
    return Lookup(key, executor);
}

template <>
inline docid_t PrimaryKeyIndexReader::LookupWithType(const autil::StringView& key,
                                                     future_lite::Executor* executor) const
{
    return Lookup(key);
}

template <>
inline docid_t PrimaryKeyIndexReader::LookupWithType(const autil::MultiChar& key, future_lite::Executor* executor) const
{
    const auto& constStr = autil::StringView(key.data(), key.size());
    return Lookup(constStr);
}
}} // namespace indexlib::index
