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

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/mem_pool/pool_allocator.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/kv/FieldValueExtractor.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVMetricsCollector.h"
#include "indexlib/index/kv/KVReadOptions.h"
#include "indexlib/util/KeyHasherTyped.h"
#include "indexlib/util/Status.h"

namespace autil {
class TimeoutTerminator;
}

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlibv2::index {

class KVIndexReader;
typedef std::shared_ptr<KVIndexReader> KVIndexReaderPtr;

enum class KVResultStatus { FOUND, NOT_FOUND, DELETED, FAIL, TIMEOUT };

struct KVResult {
public:
    KVResult(KVResultStatus status_, index::FieldValueExtractor valueExtractor_)
        : status(status_)
        , valueExtractor(std::move(valueExtractor_))
    {
    }
    KVResultStatus status;
    index::FieldValueExtractor valueExtractor;
};

inline KVResultStatus TranslateStatus(indexlib::util::Status status)
{
    switch (status) {
    case indexlib::util::OK:
        return KVResultStatus::FOUND;
    case indexlib::util::NOT_FOUND:
        return KVResultStatus::NOT_FOUND;
    case indexlib::util::DELETED:
        return KVResultStatus::DELETED;
    default:
        return KVResultStatus::FAIL;
    }
    return KVResultStatus::FAIL;
}

class KVIndexReader : public IIndexReader
{
public:
    template <typename T>
    using use_try_t = future_lite::interface::use_try_t<T>;

    using StatusPoolAlloc = autil::mem_pool::pool_allocator<use_try_t<KVResultStatus>>;
    using StatusPoolVector = std::vector<use_try_t<KVResultStatus>, StatusPoolAlloc>;

    using ResultPoolAlloc = autil::mem_pool::pool_allocator<use_try_t<KVResult>>;
    using ResultPoolVector = std::vector<use_try_t<KVResult>, ResultPoolAlloc>;

public:
    KVIndexReader(schemaid_t readerSchemaId);
    virtual ~KVIndexReader();

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const framework::TabletData* tabletData) noexcept override;

    KVResultStatus Get(const autil::StringView& key, autil::StringView& value, const KVReadOptions& options) const;
    KVResultStatus Get(index::keytype_t keyHash, autil::StringView& value, const KVReadOptions& options) const;
    KVResult Get(const autil::StringView& key, const KVReadOptions& options) const;
    KVResult Get(index::keytype_t keyHash, const KVReadOptions& options) const;

    FL_LAZY(KVResultStatus)
    GetAsync(const autil::StringView& key, autil::StringView& value, const KVReadOptions& options) const noexcept;
    FL_LAZY(KVResultStatus)
    GetAsync(index::keytype_t keyHash, autil::StringView& value, const KVReadOptions& options) const noexcept;
    FL_LAZY(KVResult)
    GetAsync(const autil::StringView& key, const KVReadOptions& options) const noexcept;
    FL_LAZY(KVResult)
    GetAsync(index::keytype_t keyHash, const KVReadOptions& options) const noexcept;

    template <typename StringAlloc = std::allocator<autil::StringView>>
    FL_LAZY(StatusPoolVector)
    BatchGetAsync(const std::vector<autil::StringView, StringAlloc>& keys,
                  std::vector<autil::StringView, StringAlloc>& values, const KVReadOptions& options) const noexcept;

    template <typename KeyTypeAlloc = std::allocator<index::keytype_t>,
              typename StringAlloc = std::allocator<autil::StringView>>
    FL_LAZY(StatusPoolVector)
    BatchGetAsync(const std::vector<index::keytype_t, KeyTypeAlloc>& keyHashes,
                  std::vector<autil::StringView, StringAlloc>& values, const KVReadOptions& options) const noexcept;

    template <typename StringAlloc = std::allocator<autil::StringView>>
    FL_LAZY(ResultPoolVector)
    BatchGetAsync(const std::vector<autil::StringView, StringAlloc>& keys, const KVReadOptions& options) const noexcept;

    static bool GetHashKeyWithType(indexlib::HashFunctionType hashType, const autil::StringView& keyStr, dictkey_t& key)
    {
        return indexlib::util::GetHashKey(hashType, keyStr, key);
    }

    const std::shared_ptr<indexlibv2::config::KVIndexConfig>& GetKVIndexConfig() const { return _kvIndexConfig; }

    index::KVValueType GetValueType() const { return _valueType; }

    schemaid_t GetReaderSchemaId() const { return _readerSchemaId; }

    indexlib::HashFunctionType GetHasherType() const { return _keyHasherType; }

    std::shared_ptr<indexlibv2::index::PackAttributeFormatter> GetPackAttributeFormatter() const { return _formatter; }

protected:
    bool GetHashKey(const autil::StringView& keyStr, dictkey_t& key) const
    {
        return GetHashKeyWithType(_keyHasherType, keyStr, key);
    }

    virtual Status DoOpen(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                          const framework::TabletData* tabletData) noexcept = 0;

    virtual FL_LAZY(KVResultStatus)
        InnerGet(const KVReadOptions* readOptions, index::keytype_t key, autil::StringView& value,
                 index::KVMetricsCollector* metricsCollector = NULL) const noexcept = 0;

    FL_LAZY(KVResultStatus)
    InnerGet(const KVReadOptions* readOptions, const autil::StringView& key, autil::StringView& value,
             index::KVMetricsCollector* metricsCollector = NULL) const noexcept
    {
        index::keytype_t hashKey = 0;
        if (GetHashKey(key, hashKey)) {
            FL_CORETURN FL_COAWAIT InnerGet(readOptions, hashKey, value, metricsCollector);
        }
        FL_CORETURN KVResultStatus::NOT_FOUND;
    }

private:
    bool InitInnerMeta(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvConfig);

    template <typename StringAlloc, typename KeysType>
    inline FL_LAZY(KVIndexReader::StatusPoolVector)
        InnerBatchGetAsync(const KeysType& keys, std::vector<autil::StringView, StringAlloc>& values,
                           const KVReadOptions& options) const noexcept;

    template <typename KeysType, typename ValuesType>
    inline FL_LAZY(KVIndexReader::ResultPoolVector)
        InnerBatchGetResultAsync(const KeysType& keys, ValuesType& values, const KVReadOptions& options) const noexcept;

protected:
    schemaid_t _readerSchemaId = DEFAULT_SCHEMAID;
    std::shared_ptr<indexlibv2::config::KVIndexConfig> _kvIndexConfig;
    indexlib::HashFunctionType _keyHasherType;
    index::KVValueType _valueType;
    int64_t _ttl;
    std::shared_ptr<indexlibv2::index::PackAttributeFormatter> _formatter;
};

inline KVResultStatus KVIndexReader::Get(const autil::StringView& key, autil::StringView& value,
                                         const KVReadOptions& options) const
{
    return future_lite::interface::syncAwait(GetAsync(key, value, options));
}

inline KVResultStatus KVIndexReader::Get(index::keytype_t key, autil::StringView& value,
                                         const KVReadOptions& options) const
{
    return future_lite::interface::syncAwait(GetAsync(key, value, options));
}

inline KVResult KVIndexReader::Get(const autil::StringView& key, const KVReadOptions& options) const
{
    autil::StringView value;
    auto status = Get(key, value, options);
    index::FieldValueExtractor extractor(_formatter.get(), std::move(value), options.pool);
    return KVResult(status, extractor);
}

inline KVResult KVIndexReader::Get(index::keytype_t keyHash, const KVReadOptions& options) const
{
    autil::StringView value;
    auto status = Get(keyHash, value, options);
    index::FieldValueExtractor extractor(_formatter.get(), std::move(value), options.pool);
    return KVResult(status, extractor);
}

inline FL_LAZY(KVResultStatus) KVIndexReader::GetAsync(const autil::StringView& key, autil::StringView& value,
                                                       const KVReadOptions& options) const noexcept
{
    FL_CORETURN FL_COAWAIT InnerGet(&options, key, value, options.metricsCollector);
}

inline FL_LAZY(KVResultStatus) KVIndexReader::GetAsync(index::keytype_t keyHash, autil::StringView& value,
                                                       const KVReadOptions& options) const noexcept
{
    FL_CORETURN FL_COAWAIT InnerGet(&options, keyHash, value, options.metricsCollector);
}

inline FL_LAZY(KVResult) KVIndexReader::GetAsync(const autil::StringView& key,
                                                 const KVReadOptions& options) const noexcept
{
    autil::StringView value;
    auto status = FL_COAWAIT InnerGet(&options, key, value, options.metricsCollector);
    index::FieldValueExtractor extractor(_formatter.get(), std::move(value), options.pool);
    FL_CORETURN KVResult(status, extractor);
}

inline FL_LAZY(KVResult) KVIndexReader::GetAsync(index::keytype_t keyHash, const KVReadOptions& options) const noexcept
{
    autil::StringView value;
    auto status = FL_COAWAIT InnerGet(&options, keyHash, value, options.metricsCollector);
    index::FieldValueExtractor extractor(_formatter.get(), std::move(value), options.pool);
    FL_CORETURN KVResult(status, extractor);
}

template <typename StringAlloc>
inline FL_LAZY(KVIndexReader::StatusPoolVector) KVIndexReader::BatchGetAsync(
    const std::vector<autil::StringView, StringAlloc>& keys, std::vector<autil::StringView, StringAlloc>& values,
    const KVReadOptions& options) const noexcept
{
    FL_CORETURN FL_COAWAIT InnerBatchGetAsync(keys, values, options);
}

template <typename StringAlloc>
inline FL_LAZY(KVIndexReader::ResultPoolVector) KVIndexReader::BatchGetAsync(
    const std::vector<autil::StringView, StringAlloc>& keys, const KVReadOptions& options) const noexcept
{
    std::vector<autil::StringView, StringAlloc> values(keys.get_allocator());
    FL_CORETURN FL_COAWAIT InnerBatchGetResultAsync(keys, values, options);
}

template <typename KeyTypeAlloc, typename StringAlloc>
inline FL_LAZY(KVIndexReader::StatusPoolVector) KVIndexReader::BatchGetAsync(
    const std::vector<index::keytype_t, KeyTypeAlloc>& keyHashes, std::vector<autil::StringView, StringAlloc>& values,
    const KVReadOptions& options) const noexcept
{
    FL_CORETURN FL_COAWAIT InnerBatchGetAsync(keyHashes, values, options);
}

template <typename StringAlloc, typename KeysType>
inline FL_LAZY(KVIndexReader::StatusPoolVector)
    KVIndexReader::InnerBatchGetAsync(const KeysType& keys, std::vector<autil::StringView, StringAlloc>& values,
                                      const KVReadOptions& options) const noexcept
{
    assert(options.pool);
    values.resize(keys.size());
    using LazyType = FL_LAZY(KVResultStatus);
    autil::mem_pool::pool_allocator<LazyType> lazyAlloc(options.pool);
    std::vector<LazyType, decltype(lazyAlloc)> lazyGroups(lazyAlloc);
    lazyGroups.reserve(keys.size());

    if (options.metricsCollector) {
        autil::mem_pool::pool_allocator<index::KVMetricsCollector> metricsAlloc(options.pool);
        std::vector<index::KVMetricsCollector, decltype(metricsAlloc)> metricsCollectors(keys.size(), metricsAlloc);
        for (size_t i = 0; i < keys.size(); ++i) {
            lazyGroups.push_back(InnerGet(&options, keys[i], values[i], &metricsCollectors[i]));
        }
        StatusPoolAlloc alloc(options.pool);
        auto res = FL_COAWAIT future_lite::interface::collectAllWindowed(options.maxConcurrency, options.yield,
                                                                         std::move(lazyGroups), alloc);
        int64_t sstable_latency = 0;
        for (auto& metricsCollector : metricsCollectors) {
            *options.metricsCollector += metricsCollector;
            sstable_latency = std::max(sstable_latency, metricsCollector.GetSSTableLatency());
        }
        options.metricsCollector->setSSTableLatency(sstable_latency);
        FL_CORETURN res;
    } else {
        for (size_t i = 0; i < keys.size(); ++i) {
            lazyGroups.push_back(InnerGet(&options, keys[i], values[i], NULL));
        }
        StatusPoolAlloc alloc(options.pool);
        FL_CORETURN FL_COAWAIT future_lite::interface::collectAll(std::move(lazyGroups), alloc);
    }
}

template <typename KeysType, typename ValuesType>
inline FL_LAZY(KVIndexReader::ResultPoolVector) KVIndexReader::InnerBatchGetResultAsync(
    const KeysType& keys, ValuesType& values, const KVReadOptions& options) const noexcept
{
    // std::allocator<autil::StringView> strAlloc;
    // std::vector<autil::StringView, decltype(strAlloc)> values(strAlloc);
    auto statPoolVec = FL_COAWAIT BatchGetAsync(keys, values, options);
    ResultPoolVector res(options.pool);
    res.reserve(statPoolVec.size());
    assert(statPoolVec.size() == values.size());
    for (size_t i = 0; i < statPoolVec.size(); i++) {
        index::FieldValueExtractor extractor(_formatter.get(), values[i], options.pool);
        KVResult result(future_lite::interface::getTryValue(statPoolVec[i]), std::move(extractor));
        res.push_back(std::move(result));
    }
    FL_CORETURN res;
}

} // namespace indexlibv2::index
