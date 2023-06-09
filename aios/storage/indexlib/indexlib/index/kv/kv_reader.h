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
#ifndef __INDEXLIB_KV_READER_H
#define __INDEXLIB_KV_READER_H

#include <memory>
#include <vector>

#include "autil/ConstString.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/codegen/codegen_object.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/AttributeValueTypeTraits.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_index_options.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/index/kv/kv_read_options.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyHasherTyped.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, KVIndexConfig);
DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);

template <typename T>
using use_try_t = future_lite::interface::use_try_t<T>;

namespace indexlib { namespace index {
class KVReader;
DEFINE_SHARED_PTR(KVReader);

class KVReader : public codegen::CodegenObject
{
public:
    using BoolPoolAlloc = autil::mem_pool::pool_allocator<use_try_t<bool>>;
    using BoolPoolVector = std::vector<use_try_t<bool>, BoolPoolAlloc>;

    KVReader();
    virtual ~KVReader();

public:
    virtual void Open(const config::KVIndexConfigPtr& kvConfig, const index_base::PartitionDataPtr& partitionData,
                      const util::SearchCachePartitionWrapperPtr& searchCache, int64_t latestIncSkipTs);

    // for single multi-region kkv reader
    void ReInit(const config::KVIndexConfigPtr& kvConfig, const index_base::PartitionDataPtr& partitionData,
                int64_t latestNeedSkipIncTs);

    bool Get(const autil::StringView& key, autil::StringView& value, uint64_t ts = 0,
             TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = NULL,
             KVMetricsCollector* metricsCollector = NULL) const
    {
        return future_lite::interface::syncAwait(GetAsync(key, value, ts, searchCacheType, pool, metricsCollector));
    }

    FL_LAZY(bool)
    GetAsync(const autil::StringView& key, autil::StringView& value, uint64_t ts = 0,
             TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = NULL,
             KVMetricsCollector* metricsCollector = NULL) const;

    bool Get(keytype_t keyHash, autil::StringView& value, uint64_t ts = 0,
             TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = NULL,
             KVMetricsCollector* metricsCollector = NULL) const
    {
        return future_lite::interface::syncAwait(GetAsync(keyHash, value, ts, searchCacheType, pool, metricsCollector));
    }

    FL_LAZY(bool)
    GetAsync(keytype_t keyHash, autil::StringView& value, uint64_t ts = 0,
             TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = NULL,
             KVMetricsCollector* metricsCollector = NULL) const;

    template <typename T>
    bool Get(const autil::StringView& key, T& value, const KVReadOptions& options) const
    {
        return future_lite::interface::syncAwait(GetAsync(key, value, options));
    }

    template <typename T>
    FL_LAZY(bool)
    GetAsync(const autil::StringView& key, T& value, const KVReadOptions& options) const;

    template <typename T>
    bool Get(keytype_t keyHash, T& value, const KVReadOptions& options) const
    {
        return future_lite::interface::syncAwait(GetAsync(keyHash, value, options));
    }

    template <typename T>
    FL_LAZY(bool)
    GetAsync(keytype_t keyHash, T& value, const KVReadOptions& options) const;

    template <typename StringAlloc = std::allocator<autil::StringView>>
    future_lite::coro::Lazy<BoolPoolVector> BatchGetAsync(const std::vector<autil::StringView, StringAlloc>& keys,
                                                          std::vector<autil::StringView, StringAlloc>& values,
                                                          TableSearchCacheType searchCacheType,
                                                          const KVReadOptions& options) const;
    template <typename KeyTypeAlloc = std::allocator<keytype_t>,
              typename StringAlloc = std::allocator<autil::StringView>>
    future_lite::coro::Lazy<BoolPoolVector> BatchGetAsync(const std::vector<keytype_t, KeyTypeAlloc>& keyHashes,
                                                          std::vector<autil::StringView, StringAlloc>& values,
                                                          TableSearchCacheType searchCacheType,
                                                          const KVReadOptions& options) const;

    virtual KVReaderPtr CreateCodegenKVReader() = 0;
    virtual KVReader* Clone() = 0;

    FL_LAZY(bool)
    IsUpdated(const autil::StringView& key, uint64_t ts, TableSearchCacheType searchCacheType = tsc_default,
              autil::mem_pool::Pool* pool = NULL, KVMetricsCollector* metricsCollector = NULL) const;

    const config::KVIndexConfigPtr& GetKVIndexConfig() const { return mKVIndexOptions.kvConfig; }
    indexlibv2::index::KVValueType GetValueType() const { return mValueType; }

    bool GetHashKey(const autil::StringView& keyStr, dictkey_t& key) const
    {
        return GetHashKeyWithType(mKeyHasherType, keyStr, key);
    }

    const common::PackAttributeFormatterPtr& GetPackAttributeFormatter() const { return mPackAttrFormatter; }

    attrid_t GetAttributeId(const std::string& fieldName) const
    {
        const config::AttributeConfigPtr& attrConfig = mPackAttrFormatter->GetAttributeConfig(fieldName);
        return attrConfig ? attrConfig->GetAttrId() : INVALID_ATTRID;
    }

    HashFunctionType GetHasherType() const { return mKeyHasherType; }

    static bool GetHashKeyWithType(HashFunctionType hashType, const autil::StringView& keyStr, dictkey_t& key)
    {
        return util::GetHashKey(hashType, keyStr, key);
    }

    void SetSearchCachePriority(autil::CacheBase::Priority priority) { mKVIndexOptions.cachePriority = priority; }

private:
    void InitInnerMeta(const config::KVIndexConfigPtr& kvConfig);

    template <typename IndexValueType, typename T>
    inline bool GetValueByReference(common::AttributeReference* attrRefBase, const char* baseAddr, T& value,
                                    autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

protected:
    virtual FL_LAZY(bool) InnerGet(const KVIndexOptions* options, keytype_t key, autil::StringView& value, uint64_t ts,
                                   TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool = NULL,
                                   KVMetricsCollector* metricsCollector = NULL) const = 0;
    virtual FL_LAZY(bool)
        InnerGet(const KVIndexOptions* options, const autil::StringView& key, autil::StringView& value, uint64_t ts,
                 TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool = NULL,
                 KVMetricsCollector* metricsCollector = NULL) const
    {
        FL_CORETURN false;
    }

protected:
    HashFunctionType mKeyHasherType;
    indexlibv2::index::KVValueType mValueType;
    common::PackAttributeFormatterPtr mPackAttrFormatter;
    KVIndexOptions mKVIndexOptions;

private:
    friend class KVReaderTest;
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////
inline FL_LAZY(bool) KVReader::GetAsync(const autil::StringView& key, autil::StringView& value, uint64_t ts,
                                        TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool,
                                        KVMetricsCollector* metricsCollector) const
{
    keytype_t hashKey;
    if (likely(GetHashKey(key, hashKey))) {
        FL_CORETURN FL_COAWAIT GetAsync(hashKey, value, ts, searchCacheType, pool, metricsCollector);
    }
    if (metricsCollector) {
        metricsCollector->EndQuery();
    }
    FL_CORETURN false;
}

inline FL_LAZY(bool) KVReader::GetAsync(keytype_t keyHash, autil::StringView& value, uint64_t ts,
                                        TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool,
                                        KVMetricsCollector* metricsCollector) const
{
    FL_CORETURN FL_COAWAIT InnerGet((KVIndexOptions*)&mKVIndexOptions, keyHash, value, ts, searchCacheType, pool,
                                    metricsCollector);
}

inline FL_LAZY(bool) KVReader::IsUpdated(const autil::StringView& key, uint64_t ts,
                                         TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool,
                                         KVMetricsCollector* metricsCollector) const
{
    KVIndexOptions options = mKVIndexOptions;
    options.ttl = 0;
    keytype_t hashKey;
    autil::StringView value;
    if (likely(GetHashKey(key, hashKey))) {
        FL_CORETURN FL_COAWAIT InnerGet(&options, hashKey, value, ts, searchCacheType, pool, metricsCollector);
    }
    FL_CORETURN true;
}

template <typename T>
inline FL_LAZY(bool) KVReader::GetAsync(const autil::StringView& key, T& value, const KVReadOptions& options) const
{
    keytype_t hashKey;
    if (likely(GetHashKey(key, hashKey))) {
        FL_CORETURN FL_COAWAIT GetAsync(hashKey, value, options);
    }
    if (options.metricsCollector) {
        options.metricsCollector->EndQuery();
    }
    FL_CORETURN false;
}

template <typename T>
inline FL_LAZY(bool) KVReader::GetAsync(keytype_t keyHash, T& value, const KVReadOptions& options) const
{
    autil::StringView rawValue;
    if (!FL_COAWAIT GetAsync(keyHash, rawValue, options.timestamp, options.searchCacheType, options.pool,
                             options.metricsCollector)) {
        FL_CORETURN false;
    }
    if (mValueType == indexlibv2::index::KVVT_TYPED) {
        value = *(T*)rawValue.data();
        FL_CORETURN true;
    }
    common::AttributeReference* attrRefBase = NULL;
    attrid_t attrId = (mValueType == indexlibv2::index::KVVT_PACKED_ONE_FIELD) ? 0u : options.attrId;
    if (attrId != INVALID_ATTRID) {
        attrRefBase = mPackAttrFormatter->GetAttributeReference(attrId);
    } else {
        assert(mValueType == indexlibv2::index::KVVT_PACKED_MULTI_FIELD);
        attrRefBase = mPackAttrFormatter->GetAttributeReference(options.fieldName);
    }
    if (!attrRefBase) {
        FL_CORETURN false;
    }
    FL_CORETURN GetValueByReference<typename AttributeValueTypeTraits<T>::IndexValueType>(attrRefBase, rawValue.data(),
                                                                                          value, options.pool);
}

template <typename IndexValueType, typename T>
inline bool KVReader::GetValueByReference(common::AttributeReference* attrRefBase, const char* baseAddr, T& value,
                                          autil::mem_pool::Pool* pool) const
{
    assert(dynamic_cast<common::AttributeReferenceTyped<IndexValueType>*>(attrRefBase) != NULL);
    common::AttributeReferenceTyped<IndexValueType>* ref =
        static_cast<common::AttributeReferenceTyped<IndexValueType>*>(attrRefBase);
    return ref->GetValue(baseAddr, value, pool);
}

#define BATCH_GET_IMPL_MACRO(keyVec)                                                                                   \
    assert(options.pool);                                                                                              \
    values.resize(keyVec.size());                                                                                      \
    using LazyType = FL_LAZY(bool);                                                                                    \
    autil::mem_pool::pool_allocator<LazyType> lazyAlloc(options.pool);                                                 \
    std::vector<LazyType, decltype(lazyAlloc)> lazyGroups(lazyAlloc);                                                  \
    lazyGroups.reserve(keyVec.size());                                                                                 \
                                                                                                                       \
    if (options.metricsCollector) {                                                                                    \
        autil::mem_pool::pool_allocator<KVMetricsCollector> metricsAlloc(options.pool);                                \
        std::vector<KVMetricsCollector, decltype(metricsAlloc)> metricsCollectors(keyVec.size(), metricsAlloc);        \
        for (size_t i = 0; i < keyVec.size(); ++i) {                                                                   \
            lazyGroups.push_back(InnerGet(&mKVIndexOptions, keyVec[i], values[i], options.timestamp, searchCacheType,  \
                                          options.pool, &metricsCollectors[i]));                                       \
        }                                                                                                              \
        BoolPoolAlloc alloc(options.pool);                                                                             \
        auto res = FL_COAWAIT future_lite::interface::collectAll(std::move(lazyGroups), alloc);                        \
        for (auto& metricsCollector : metricsCollectors) {                                                             \
            *options.metricsCollector += metricsCollector;                                                             \
        }                                                                                                              \
        co_return res;                                                                                                 \
    } else {                                                                                                           \
        for (size_t i = 0; i < keyVec.size(); ++i) {                                                                   \
            lazyGroups.push_back(InnerGet(&mKVIndexOptions, keyVec[i], values[i], options.timestamp, searchCacheType,  \
                                          options.pool, NULL));                                                        \
        }                                                                                                              \
        BoolPoolAlloc alloc(options.pool);                                                                             \
        co_return FL_COAWAIT future_lite::interface::collectAll(std::move(lazyGroups), alloc);                         \
    }

template <typename StringAlloc>
inline future_lite::coro::Lazy<KVReader::BoolPoolVector>
KVReader::BatchGetAsync(const std::vector<autil::StringView, StringAlloc>& keys,
                        std::vector<autil::StringView, StringAlloc>& values, TableSearchCacheType searchCacheType,
                        const KVReadOptions& options) const
{
    BATCH_GET_IMPL_MACRO(keys);
}

template <typename KeyTypeAlloc, typename StringAlloc>
inline future_lite::coro::Lazy<KVReader::BoolPoolVector>
KVReader::BatchGetAsync(const std::vector<keytype_t, KeyTypeAlloc>& keyHashes,
                        std::vector<autil::StringView, StringAlloc>& values, TableSearchCacheType searchCacheType,
                        const KVReadOptions& options) const
{
    BATCH_GET_IMPL_MACRO(keyHashes);
}

#undef BATCH_GET_IMPL_MACRO

}} // namespace indexlib::index

#endif //__INDEXLIB_KV_READER_H
