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
#include <vector>

#include "autil/ConstString.h"
#include "autil/mem_pool/PoolVector.h"
#include "autil/mem_pool/pool_allocator.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/search/KKVIterator.h"
#include "indexlib/table/kkv_table/KKVIndexOptions.h"
#include "indexlib/table/kkv_table/KKVReadOptions.h"
#include "indexlib/util/KeyHasherTyped.h"

namespace indexlibv2::table {

class BatchKKVResult
{
public:
    template <typename T>
    using use_try_t = future_lite::interface::use_try_t<T>;

    using AllocatorType = autil::mem_pool::pool_allocator<use_try_t<index::KKVIterator*>>;
    using KKVIteratorVectorType = std::vector<use_try_t<index::KKVIterator*>, AllocatorType>;
    using ResultIterator = KKVIteratorVectorType::iterator;

public:
    BatchKKVResult(KKVReadOptions& readOptions)
        : _readOptions(readOptions)
        , _metricsCollectors(readOptions.pool)
        , _newReadOptions(readOptions.pool)
        , _kkvIterators(AllocatorType(readOptions.pool))
    {
    }

    ResultIterator Begin() { return _kkvIterators.begin(); }
    ResultIterator End() { return _kkvIterators.end(); }

    void Finish()
    {
        if (_readOptions.metricsCollector) {
            for (auto& metricCollector : _metricsCollectors) {
                metricCollector.EndQuery();
            }
            // NOTE: get the sum of each metric collector
            int64_t sstable_latency = 0;
            for (auto& metricsCollector : _metricsCollectors) {
                *_readOptions.metricsCollector += metricsCollector;
                sstable_latency = std::max(sstable_latency, metricsCollector.GetSSTableLatency());
            }
            _readOptions.metricsCollector->setSSTableLatency(sstable_latency);
        }
    }

    void Finish(size_t cursor)
    {
        if (_readOptions.metricsCollector && cursor < _metricsCollectors.size()) {
            auto& metricsCollector = _metricsCollectors[cursor];
            metricsCollector.EndQuery();
            *_readOptions.metricsCollector += metricsCollector;
            if (_readOptions.metricsCollector->GetSSTableLatency() < metricsCollector.GetSSTableLatency()) {
                _readOptions.metricsCollector->setSSTableLatency(metricsCollector.GetSSTableLatency());
            }
        }
    }

    void Resize(size_t size)
    {
        _metricsCollectors.resize(size);
        _newReadOptions.resize(size);
        for (size_t i = 0; i < size; i++) {
            _newReadOptions[i] = _readOptions;
            _newReadOptions[i].metricsCollector = &_metricsCollectors[i];
        }
    }

    auto& GetNewReadOption(size_t i)
    {
        assert(i < _newReadOptions.size());
        return _newReadOptions[i];
    }

    void SetKKVIterators(KKVIteratorVectorType&& kkvIterators) { _kkvIterators.swap(kkvIterators); }

    size_t GetKKVIteratorSize() { return _kkvIterators.size(); }

private:
    KKVReadOptions _readOptions;
    autil::mem_pool::PoolVector<index::KVMetricsCollector> _metricsCollectors;
    autil::mem_pool::PoolVector<KKVReadOptions> _newReadOptions;
    KKVIteratorVectorType _kkvIterators;
};

class KKVReader : public index::IIndexReader
{
public:
    template <typename Alloc = std::allocator<uint64_t>>
    using SKeyHashVec = std::vector<uint64_t, Alloc>;
    using SKeyHashPoolVec = SKeyHashVec<autil::mem_pool::pool_allocator<uint64_t>>;

    template <typename Alloc = std::allocator<autil::StringView>>
    using SKeyStringVec = std::vector<autil::StringView, Alloc>;
    using SKeyStringPoolVec = SKeyStringVec<autil::mem_pool::pool_allocator<autil::StringView>>;

    template <typename T>
    using use_try_t = BatchKKVResult::use_try_t<T>;

public:
    KKVReader(schemaid_t readerSchemaId);
    virtual ~KKVReader();

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const framework::TabletData* tabletData) noexcept override;
    schemaid_t GetReaderSchemaId() const { return _readerSchemaId; }
    KKVIndexOptions& GetIndexOptions() { return _indexOptions; }
    const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& GetIndexConfig() const { return _indexConfig; }

protected:
    virtual Status DoOpen(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                          const framework::TabletData* tabletData) noexcept = 0;

public:
    // =================  sync interface api  =================
    index::KKVIterator* Lookup(index::PKeyType pkeyHash, const SKeyHashVec<>& skeyHashVec, KKVReadOptions& readOptions)
    {
        return future_lite::interface::syncAwait(LookupAsync(pkeyHash, skeyHashVec, readOptions));
    }
    index::KKVIterator* Lookup(autil::StringView pkey, const SKeyStringVec<>& skeyStringVec,
                               KKVReadOptions& readOptions)
    {
        return future_lite::interface::syncAwait(LookupAsync(pkey, skeyStringVec, readOptions));
    }

    // =================  async interface api  =================
    virtual FL_LAZY(index::KKVIterator*)
        LookupAsync(index::PKeyType pkeyHash, const SKeyHashVec<>& skeyHashVec, KKVReadOptions& readOptions) = 0;

    virtual FL_LAZY(index::KKVIterator*)
        LookupAsync(index::PKeyType pkeyHash, const SKeyHashPoolVec& skeyHashVec, KKVReadOptions& readOptions) = 0;

    virtual FL_LAZY(index::KKVIterator*)
        LookupAsync(const autil::StringView& pkey, const SKeyStringVec<>& skeyStringVec,
                    KKVReadOptions& readOptions) = 0;

    virtual FL_LAZY(index::KKVIterator*)
        LookupAsync(const autil::StringView& pkey, const SKeyStringPoolVec& skeyStringVec,
                    KKVReadOptions& readOptions) = 0;

    FL_LAZY(index::KKVIterator*)
    LookupAsync(const autil::StringView& pkey, KKVReadOptions& readOptions);

    FL_LAZY(index::KKVIterator*)
    LookupAsync(index::PKeyType pkeyHash, KKVReadOptions& readOptions);

    // =================  batch async interface api  =================
    // ATTENTION: BatchKKVResult hold reference to readOptions
    template <typename PKeyIter, typename SKeyVecIter>
    FL_LAZY(BatchKKVResult*)
    BatchLookupAsync(const PKeyIter& pkeyBegin, const PKeyIter& pkeyEnd, const SKeyVecIter& skeyBegin,
                     const SKeyVecIter& skeyEnd, KKVReadOptions& readOptions);

    // // =================  other api  =================
    const indexlib::config::SortParams& GetSortParams() const { return _indexOptions.GetSortParams(); }

private:
    schemaid_t _readerSchemaId = DEFAULT_SCHEMAID;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    KKVIndexOptions _indexOptions;
};

inline FL_LAZY(index::KKVIterator*) KKVReader::LookupAsync(const autil::StringView& pkey, KKVReadOptions& readOptions)
{
    SKeyStringVec<> skeyStringVec;
    FL_CORETURN FL_COAWAIT LookupAsync(pkey, skeyStringVec, readOptions);
}

inline FL_LAZY(index::KKVIterator*) KKVReader::LookupAsync(index::PKeyType pkeyHash, KKVReadOptions& readOptions)
{
    SKeyHashVec<> skeyHashVec;
    FL_CORETURN FL_COAWAIT LookupAsync(pkeyHash, skeyHashVec, readOptions);
}

template <typename PKeyIter, typename SKeyVecIter>
inline FL_LAZY(BatchKKVResult*) KKVReader::BatchLookupAsync(const PKeyIter& pkeyBegin, const PKeyIter& pkeyEnd,
                                                            const SKeyVecIter& skeyBegin, const SKeyVecIter& skeyEnd,
                                                            KKVReadOptions& readOptions)
{
    using LazyType = FL_LAZY(index::KKVIterator*);
    autil::mem_pool::pool_allocator<LazyType> lazyAlloc(readOptions.pool);
    std::vector<LazyType, decltype(lazyAlloc)> queryGroups(lazyAlloc);
    auto pkeyIter = pkeyBegin;
    auto skeyIter = skeyBegin;
    auto pkeyCount = distance(pkeyBegin, pkeyEnd);
    if (readOptions.metricsCollector) {
        BatchKKVResult* batchResult = POOL_COMPATIBLE_NEW_CLASS(readOptions.pool, BatchKKVResult, readOptions);
        batchResult->Resize(pkeyCount);
        size_t pkeyPos = 0;
        while (pkeyIter != pkeyEnd) {
            assert(skeyIter != skeyEnd);
            queryGroups.push_back(this->LookupAsync(*pkeyIter, *skeyIter, batchResult->GetNewReadOption(pkeyPos)));
            pkeyIter++;
            skeyIter++;
            pkeyPos++;
        }
        assert(skeyIter == skeyEnd);
        auto kkvIterators = FL_COAWAIT future_lite::interface::collectAll(
            std::move(queryGroups), BatchKKVResult::AllocatorType(readOptions.pool));
        batchResult->SetKKVIterators(std::move(kkvIterators));
        FL_CORETURN batchResult;
    } else {
        while (pkeyIter != pkeyEnd) {
            assert(skeyIter != skeyEnd);
            queryGroups.push_back(this->LookupAsync(*pkeyIter, *skeyIter, readOptions));
            pkeyIter++;
            skeyIter++;
        }
        assert(skeyIter == skeyEnd);
        auto kkvIterators = FL_COAWAIT future_lite::interface::collectAll(
            std::move(queryGroups), BatchKKVResult::AllocatorType(readOptions.pool));
        BatchKKVResult* batchResult = POOL_COMPATIBLE_NEW_CLASS(readOptions.pool, BatchKKVResult, readOptions);
        batchResult->SetKKVIterators(std::move(kkvIterators));
        FL_CORETURN batchResult;
    }
}

} // namespace indexlibv2::table
