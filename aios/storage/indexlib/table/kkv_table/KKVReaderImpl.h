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

#include <vector>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/kkv/building/KKVBuildingSegmentReader.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentReader.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/index/kkv/search/KKVIterator.h"
#include "indexlib/index/kkv/search/KKVSearchCoroutine.h"
#include "indexlib/index/kkv/search/NormalKKVIteratorImpl.h"
#include "indexlib/table/kkv_table/KKVIndexOptions.h"
#include "indexlib/table/kkv_table/KKVReadOptions.h"
#include "indexlib/table/kkv_table/KKVReader.h"
#include "indexlib/util/ShardUtil.h"
#include "indexlib/util/Status.h"

namespace indexlibv2::table {

template <typename SKeyType>
class KKVReaderImpl : public KKVReader
{
public:
    using NormalKKVIteratorImplTyped = index::NormalKKVIteratorImpl<SKeyType>;
    using BuiltSegmentReaderTyped = index::KKVBuiltSegmentReader<SKeyType>;
    using BuiltSegmentReaderPtr = std::shared_ptr<BuiltSegmentReaderTyped>;
    using DiskShardReaderAndLocator = std::pair<BuiltSegmentReaderPtr, std::shared_ptr<framework::Locator>>;
    using DiskShardReaderVector = std::vector<std::vector<DiskShardReaderAndLocator>>;
    using BuildingSegmentReaderTyped = index::KKVBuildingSegmentReader<SKeyType>;
    using BuildingSegmentReaderPtr = std::shared_ptr<BuildingSegmentReaderTyped>;
    using MemShardReaderAndLocator = std::pair<BuildingSegmentReaderPtr, std::shared_ptr<framework::Locator>>;
    using MemShardReaderVector = std::vector<std::vector<MemShardReaderAndLocator>>;

public:
    KKVReaderImpl(schemaid_t readerSchemaId) : KKVReader(readerSchemaId) {}

protected:
    Status DoOpen(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                  const framework::TabletData* tabletData) noexcept override;
    FL_LAZY(index::KKVIterator*)
    LookupAsync(index::PKeyType pkeyHash, const SKeyHashVec<>& skeyHashVec, KKVReadOptions& readOptions) override;

    FL_LAZY(index::KKVIterator*)
    LookupAsync(index::PKeyType pkeyHash, const SKeyHashPoolVec& skeyHashVec, KKVReadOptions& readOptions) override;

    FL_LAZY(index::KKVIterator*)
    LookupAsync(const autil::StringView& pkey, const SKeyStringVec<>& skeyStringVec,
                KKVReadOptions& readOptions) override;

    FL_LAZY(index::KKVIterator*)
    LookupAsync(const autil::StringView& pkey, const SKeyStringPoolVec& skeyStringVec,
                KKVReadOptions& readOptions) override;
    void ResetCounter(index::KVMetricsCollector* metricsCollector) const;

private:
    template <typename Alloc>
    FL_LAZY(index::KKVIterator*)
    LookupAsync(index::PKeyType pkeyHash, const SKeyHashVec<Alloc>& skeyHashVec, KKVReadOptions& readOptions);

    template <typename Alloc>
    FL_LAZY(index::KKVIterator*)
    LookupAsyncWithStringPKey(const autil::StringView& pkey, const SKeyStringVec<Alloc>& skeyStringVec,
                              KKVReadOptions& readOptions);

    template <typename Alloc>
    FL_LAZY(index::KKVIterator*)
    LookupAsyncWithStringPKey(const autil::StringView& pkey, const SKeyHashVec<Alloc>& skeyHashVec,
                              KKVReadOptions& readOptions);

    template <typename Alloc>
    FL_LAZY(index::KKVIterator*)
    LookupAsync(index::PKeyType pkeyHash, const SKeyStringVec<Alloc>& skeyStringVec, KKVReadOptions& readOptions);

private:
    Status LoadSegmentReader(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                             const framework::TabletData* tabletData) noexcept;

    Status LoadMemSegmentReader(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                                const framework::TabletData* tabletData) noexcept;

    Status AddMemSegments(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                          framework::TabletData::Slice& slice) noexcept;
    Status LoadMemSegmentReader(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                                framework::Segment* segment) noexcept;
    Status LoadDiskSegmentReader(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& indexConfig,
                                 const framework::TabletData* tabletData);
    void AlignSegmentReaders(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& indexConfig);

protected:
    virtual FL_LAZY(index::KKVIterator*) InnerLookup(index::PKeyType pkeyHash, const std::vector<uint64_t>& skeyHashVec,
                                                     KKVReadOptions& readOptions, KKVIndexOptions& indexOptions);

    virtual FL_LAZY(index::KKVIterator*)
        InnerLookup(index::PKeyType pkeyHash,
                    const std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>>& skeyHashVec,
                    KKVReadOptions& readOptions, KKVIndexOptions& indexOptions);

private:
    template <typename Allocator>
    FL_LAZY(index::KKVIterator*)
    InnerLookupImpl(index::PKeyType pkeyHash, const std::vector<uint64_t, Allocator>& skeyHashVec,
                    KKVReadOptions& readOptions, KKVIndexOptions& indexOptions);

protected:
    size_t GetShardId(index::PKeyType key) const;
    bool GetPKeyHash(const autil::StringView& pkey, index::PKeyType& result);
    bool GetSKeyHash(const autil::StringView& skey, uint64_t& result);

    template <typename inputAlloc = std::allocator<autil::StringView>, typename outputAlloc = std::allocator<uint64_t>>
    bool GetSKeyHashVec(const SKeyStringVec<inputAlloc>& skeys, SKeyHashVec<outputAlloc>& skeyHashs);

public:
    MemShardReaderVector& TEST_GetMemoryShardReaders();
    DiskShardReaderVector& TEST_GetDiskShardReaders();

protected:
    bool _hasTTL = false;
    bool _hasSegReader = false;
    size_t _shardCount = 0;
    FieldType _skeyDictFieldType;
    indexlib::HashFunctionType _pkeyHasherType;
    indexlib::HashFunctionType _skeyHasherType;

    MemShardReaderVector _memShardReaders;
    DiskShardReaderVector _diskShardReaders;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.table, KKVReaderImpl, SKeyType);

///////////////////////////////////////////////////////////////////////

template <typename SKeyType>
FL_LAZY(index::KKVIterator*)
KKVReaderImpl<SKeyType>::LookupAsync(const autil::StringView& pkey, const SKeyStringVec<>& skeyStringVec,
                                     KKVReadOptions& readOptions)
{
    FL_CORETURN FL_COAWAIT LookupAsyncWithStringPKey(pkey, skeyStringVec, readOptions);
}

template <typename SKeyType>
FL_LAZY(index::KKVIterator*)
KKVReaderImpl<SKeyType>::LookupAsync(const autil::StringView& pkey, const SKeyStringPoolVec& skeyStringVec,
                                     KKVReadOptions& readOptions)
{
    FL_CORETURN FL_COAWAIT LookupAsyncWithStringPKey(pkey, skeyStringVec, readOptions);
}

template <typename SKeyType>
template <typename Alloc>
FL_LAZY(index::KKVIterator*)
KKVReaderImpl<SKeyType>::LookupAsyncWithStringPKey(const autil::StringView& pkey,
                                                   const SKeyStringVec<Alloc>& skeyStringVec,
                                                   KKVReadOptions& readOptions)

{
    index::PKeyType pkeyHash = (index::PKeyType)0;
    if (!GetPKeyHash(pkey, pkeyHash)) {
        if (readOptions.metricsCollector) {
            readOptions.metricsCollector->EndQuery();
        }
        FL_CORETURN nullptr;
    }
    FL_CORETURN FL_COAWAIT LookupAsync(pkeyHash, skeyStringVec, readOptions);
}

template <typename SKeyType>
template <typename Alloc>
FL_LAZY(index::KKVIterator*)
KKVReaderImpl<SKeyType>::LookupAsync(index::PKeyType pkeyHash, const SKeyStringVec<Alloc>& skeyStringVec,
                                     KKVReadOptions& readOptions)
{
    assert(readOptions.pool);
    autil::mem_pool::pool_allocator<uint64_t> alloc(readOptions.pool);
    SKeyHashVec<autil::mem_pool::pool_allocator<uint64_t>> skeyHashVec(alloc);
    if (!GetSKeyHashVec(skeyStringVec, skeyHashVec)) {
        if (readOptions.metricsCollector) {
            readOptions.metricsCollector->EndQuery();
        }
        FL_CORETURN nullptr;
    }
    FL_CORETURN FL_COAWAIT InnerLookup(pkeyHash, skeyHashVec, readOptions, GetIndexOptions());
}

template <typename SKeyType>
template <typename Alloc>
FL_LAZY(index::KKVIterator*)
KKVReaderImpl<SKeyType>::LookupAsyncWithStringPKey(const autil::StringView& pkey, const SKeyHashVec<Alloc>& skeyHashVec,
                                                   KKVReadOptions& readOptions)
{
    index::PKeyType pkeyHash = (index::PKeyType)0;
    if (!GetPKeyHash(pkey, pkeyHash)) {
        if (readOptions.metricsCollector) {
            readOptions.metricsCollector->EndQuery();
        }
        FL_CORETURN nullptr;
    }
    FL_CORETURN FL_COAWAIT LookupAsync(pkeyHash, skeyHashVec, readOptions);
}

template <typename SKeyType>
FL_LAZY(index::KKVIterator*)
KKVReaderImpl<SKeyType>::LookupAsync(index::PKeyType pkeyHash, const SKeyHashVec<>& skeyHashVec,
                                     KKVReadOptions& readOptions)
{
    FL_CORETURN FL_COAWAIT InnerLookup(pkeyHash, skeyHashVec, readOptions, this->GetIndexOptions());
}

template <typename SKeyType>
FL_LAZY(index::KKVIterator*)
KKVReaderImpl<SKeyType>::LookupAsync(index::PKeyType pkeyHash, const SKeyHashPoolVec& skeyHashVec,
                                     KKVReadOptions& readOptions)
{
    FL_CORETURN FL_COAWAIT InnerLookup(pkeyHash, skeyHashVec, readOptions, this->GetIndexOptions());
}

template <typename SKeyType>
inline FL_LAZY(index::KKVIterator*) KKVReaderImpl<SKeyType>::InnerLookup(index::PKeyType pkeyHash,
                                                                         const std::vector<uint64_t>& skeyHashVec,
                                                                         KKVReadOptions& readOptions,
                                                                         KKVIndexOptions& indexOptions)
{
    FL_CORETURN FL_COAWAIT InnerLookupImpl(pkeyHash, skeyHashVec, readOptions, indexOptions);
}

template <typename SKeyType>
inline FL_LAZY(index::KKVIterator*) KKVReaderImpl<SKeyType>::InnerLookup(
    index::PKeyType pkeyHash, const std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>>& skeyHashVec,
    KKVReadOptions& readOptions, KKVIndexOptions& indexOptions)
{
    FL_CORETURN FL_COAWAIT InnerLookupImpl(pkeyHash, skeyHashVec, readOptions, indexOptions);
}

template <typename SKeyType>
template <typename Allocator>
inline FL_LAZY(index::KKVIterator*) KKVReaderImpl<SKeyType>::InnerLookupImpl(
    index::PKeyType pkeyHash, const std::vector<uint64_t, Allocator>& skeyHashVec, KKVReadOptions& readOptions,
    KKVIndexOptions& indexOptions)
{
    auto metricsCollector = readOptions.metricsCollector;
    if (!_hasSegReader) {
        if (metricsCollector) {
            metricsCollector->EndQuery();
        }
        FL_CORETURN POOL_COMPATIBLE_NEW_CLASS(readOptions.pool, index::KKVIterator, readOptions.pool);
    }

    uint64_t currentTimeInSecond = autil::TimeUtility::us2sec(readOptions.timestamp);
    size_t columnId = GetShardId(pkeyHash);
    assert(columnId < _diskShardReaders.size() && columnId < _memShardReaders.size());
    const auto& buildingSegReaders = _memShardReaders[columnId];
    const auto& builtSegReaders = _diskShardReaders[columnId];
    if (UNLIKELY(buildingSegReaders.empty() && builtSegReaders.empty())) {
        if (metricsCollector) {
            metricsCollector->EndQuery();
        }
        FL_CORETURN POOL_COMPATIBLE_NEW_CLASS(readOptions.pool, index::KKVIterator, readOptions.pool);
    }
    index::KKVIteratorImplBase* iter = nullptr;
    auto pool = readOptions.pool;
    if (!future_lite::interface::USE_COROUTINES) {
        iter = POOL_COMPATIBLE_NEW_CLASS(pool, NormalKKVIteratorImplTyped, pool, indexOptions.GetIndexConfig().get(),
                                         indexOptions.GetTTL(), pkeyHash, std::move(skeyHashVec), builtSegReaders,
                                         buildingSegReaders, currentTimeInSecond, metricsCollector);
    } else {
        auto ret = FL_COAWAIT index::KKVSearchCoroutine<SKeyType>::Search(
            pool, indexOptions.GetIndexConfig().get(), indexOptions.GetTTL(), !indexOptions.GetSortParams().empty(),
            indexOptions.GetSKeyCountLimits(), pkeyHash, std::move(skeyHashVec), builtSegReaders, buildingSegReaders,
            currentTimeInSecond, metricsCollector);
        if (!ret.first.IsOK()) {
            // TODO(qisa.cb) should return status
            FL_CORETURN nullptr;
        }
        iter = ret.second;
    }
    index::KKVIterator* kkvIter =
        POOL_COMPATIBLE_NEW_CLASS(pool, index::KKVIterator, pool, iter, indexOptions.GetIndexConfig().get(),
                                  indexOptions.GetPlainFormatEncoder().get(), !indexOptions.GetSortParams().empty(),
                                  metricsCollector, indexOptions.GetSKeyCountLimits());
    FL_CORETURN kkvIter;
}

template <typename SKeyType>
inline size_t KKVReaderImpl<SKeyType>::GetShardId(index::PKeyType key) const
{
    return indexlib::util::ShardUtil::GetShardId(key, _shardCount);
}

template <typename SKeyType>
inline bool KKVReaderImpl<SKeyType>::GetPKeyHash(const autil::StringView& pkey, index::PKeyType& result)
{
    dictkey_t key = 0;
    if (unlikely(!indexlib::util::GetHashKey(_pkeyHasherType, pkey, key))) {
        AUTIL_LOG(ERROR, "hash prefix key [%s] of type[%d] failed", pkey.data(), _pkeyHasherType);
        return false;
    }
    result = (index::PKeyType)key;
    return true;
}

template <typename SKeyType>
inline bool KKVReaderImpl<SKeyType>::GetSKeyHash(const autil::StringView& skey, uint64_t& result)
{
    dictkey_t key = 0;
    if (unlikely(!indexlib::util::GetHashKey(_skeyHasherType, skey, key))) {
        AUTIL_LOG(ERROR, "hash suffix key [%s] of type[%d] failed", skey.data(), _skeyHasherType);
        return false;
    }
    result = (uint64_t)key;
    return true;
}

template <typename SKeyType>
template <typename inputAlloc, typename outputAlloc>
inline bool KKVReaderImpl<SKeyType>::GetSKeyHashVec(const SKeyStringVec<inputAlloc>& skeys,
                                                    SKeyHashVec<outputAlloc>& skeyHashs)
{
    assert(skeyHashs.empty());
    size_t size = skeys.size();
    skeyHashs.reserve(size);
    for (size_t i = 0; i < size; ++i) {
        uint64_t skey;
        if (GetSKeyHash(skeys[i], skey)) {
            skeyHashs.push_back(skey);
        }
    }
    return !skeyHashs.empty() || skeys.empty();
}

template <typename SKeyType>
inline void KKVReaderImpl<SKeyType>::ResetCounter(index::KVMetricsCollector* metricsCollector) const
{
    if (!metricsCollector) {
        return;
    }
    metricsCollector->BeginMemTableQuery();
    metricsCollector->ResetBlockCounter();
}

MARK_EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(KKVReaderImpl);

} // namespace indexlibv2::table
