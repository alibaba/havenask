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
#ifndef __INDEXLIB_KKV_READER_H
#define __INDEXLIB_KKV_READER_H

#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/pool_allocator.h"
#include "future_lite/CoroInterface.h"
#include "indexlib/codegen/codegen_object.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_read_options.h"
#include "indexlib/index/kkv/kkv_reader_impl.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);

namespace indexlib { namespace index {
class KKVReader;
DEFINE_SHARED_PTR(KKVReader);

class KKVReader : public codegen::CodegenObject
{
public:
    virtual ~KKVReader() {}
    KKVReader();
    KKVReader(const KKVReader& rhs) : codegen::CodegenObject(rhs)
    {
        mKKVIndexOptions = rhs.mKKVIndexOptions;
        mPrefixKeyHasherType = rhs.mPrefixKeyHasherType;
        mSuffixKeyHasherType = rhs.mSuffixKeyHasherType;
    }

    virtual KKVReaderPtr CreateCodegenKKVReader() = 0;

    virtual void Open(const config::KKVIndexConfigPtr& kkvConfig, const index_base::PartitionDataPtr& partitionData,
                      const util::SearchCachePartitionWrapperPtr& searchCache, int64_t latestNeedSkipIncTs,
                      const std::string& schemaName = "") = 0;

    // for single multi-region kkv reader
    virtual void ReInit(const config::KKVIndexConfigPtr& kkvConfig, const index_base::PartitionDataPtr& partitionData,
                        int64_t latestNeedSkipIncTs) = 0;

    virtual KKVReader* Clone() = 0;

    const config::KKVIndexConfigPtr& GetKKVIndexConfig() const { return mKKVIndexOptions.kkvConfig; }

    const config::SortParams& GetSortParams() const { return mKKVIndexOptions.sortParams; }

    void SetSearchCachePriority(autil::CacheBase::Priority priority) { mKKVIndexOptions.cachePriority = priority; }

    KKVIterator* Lookup(PKeyType pkeyHash,
                        std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>> suffixKeyHashVec,
                        uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default,
                        autil::mem_pool::Pool* pool = nullptr, KVMetricsCollector* metricsCollector = nullptr)
    {
        return future_lite::interface::syncAwait(
            LookupAsync(pkeyHash, suffixKeyHashVec, timestamp, searchCacheType, pool, metricsCollector));
    }

    virtual FL_LAZY(KKVIterator*)
        LookupAsync(PKeyType pkeyHash,
                    std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>> suffixKeyHashVec,
                    uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default,
                    autil::mem_pool::Pool* pool = nullptr, KVMetricsCollector* metricsCollector = nullptr) = 0;

    KKVIterator* Lookup(PKeyType pkeyHash, std::vector<uint64_t> suffixKeyHashVec, uint64_t timestamp,
                        TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = nullptr,
                        KVMetricsCollector* metricsCollector = nullptr)
    {
        return future_lite::interface::syncAwait(
            LookupAsync(pkeyHash, suffixKeyHashVec, timestamp, searchCacheType, pool, metricsCollector));
    }

    virtual FL_LAZY(KKVIterator*)
        LookupAsync(PKeyType pkeyHash, std::vector<uint64_t> suffixKeyHashVec, uint64_t timestamp,
                    TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = nullptr,
                    KVMetricsCollector* metricsCollector = nullptr) = 0;

    KKVIterator* Lookup(const autil::StringView& prefixKey, uint64_t timestamp,
                        TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = nullptr,
                        KVMetricsCollector* metricsCollector = nullptr)
    {
        return future_lite::interface::syncAwait(
            LookupAsync(prefixKey, timestamp, searchCacheType, pool, metricsCollector));
    }

    virtual FL_LAZY(KKVIterator*)
        LookupAsync(const autil::StringView& prefixKey, uint64_t timestamp,
                    TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = nullptr,
                    KVMetricsCollector* metricsCollector = nullptr) = 0;

    KKVIterator*
    Lookup(const autil::StringView& prefixKey,
           const std::vector<autil::StringView, autil::mem_pool::pool_allocator<autil::StringView>>& suffixKeys,
           uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default,
           autil::mem_pool::Pool* pool = nullptr, KVMetricsCollector* metricsCollector = nullptr)
    {
        return future_lite::interface::syncAwait(
            LookupAsync(prefixKey, suffixKeys, timestamp, searchCacheType, pool, metricsCollector));
    }

    virtual FL_LAZY(KKVIterator*) LookupAsync(
        const autil::StringView& prefixKey,
        const std::vector<autil::StringView, autil::mem_pool::pool_allocator<autil::StringView>>& suffixKeys,
        uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = nullptr,
        KVMetricsCollector* metricsCollector = nullptr) = 0;

    KKVIterator* Lookup(const autil::StringView& prefixKey, const std::vector<autil::StringView>& suffixKeys,
                        uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default,
                        autil::mem_pool::Pool* pool = nullptr, KVMetricsCollector* metricsCollector = nullptr)
    {
        return future_lite::interface::syncAwait(
            LookupAsync(prefixKey, suffixKeys, timestamp, searchCacheType, pool, metricsCollector));
    }

    virtual FL_LAZY(KKVIterator*)
        LookupAsync(const autil::StringView& prefixKey, const std::vector<autil::StringView>& suffixKeys,
                    uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default,
                    autil::mem_pool::Pool* pool = nullptr, KVMetricsCollector* metricsCollector = nullptr) = 0;

    KKVIterator*
    Lookup(PKeyType pkeyHash,
           const std::vector<autil::StringView, autil::mem_pool::pool_allocator<autil::StringView>>& suffixKeys,
           uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default,
           autil::mem_pool::Pool* pool = nullptr, KVMetricsCollector* metricsCollector = nullptr)
    {
        return future_lite::interface::syncAwait(
            LookupAsync(pkeyHash, suffixKeys, timestamp, searchCacheType, pool, metricsCollector));
    }

    virtual FL_LAZY(KKVIterator*) LookupAsync(
        PKeyType pkeyHash,
        const std::vector<autil::StringView, autil::mem_pool::pool_allocator<autil::StringView>>& suffixKeys,
        uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = nullptr,
        KVMetricsCollector* metricsCollector = nullptr) = 0;

    KKVIterator* Lookup(PKeyType pkeyHash, const std::vector<autil::StringView>& suffixKeys, uint64_t timestamp,
                        TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = nullptr,
                        KVMetricsCollector* metricsCollector = nullptr)
    {
        return future_lite::interface::syncAwait(
            LookupAsync(pkeyHash, suffixKeys, timestamp, searchCacheType, pool, metricsCollector));
    }

    virtual FL_LAZY(KKVIterator*)
        LookupAsync(PKeyType pkeyHash, const std::vector<autil::StringView>& suffixKeys, uint64_t timestamp,
                    TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = nullptr,
                    KVMetricsCollector* metricsCollector = nullptr) = 0;

    KKVIterator* Lookup(PKeyType pkeyHash, uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default,
                        autil::mem_pool::Pool* pool = nullptr, KVMetricsCollector* metricsCollector = nullptr)
    {
        return future_lite::interface::syncAwait(
            LookupAsync(pkeyHash, timestamp, searchCacheType, pool, metricsCollector));
    }

    virtual FL_LAZY(KKVIterator*)
        LookupAsync(PKeyType pkeyHash, uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default,
                    autil::mem_pool::Pool* pool = nullptr, KVMetricsCollector* metricsCollector = nullptr) = 0;

protected:
    KKVIndexOptions mKKVIndexOptions;
    HashFunctionType mPrefixKeyHasherType;
    HashFunctionType mSuffixKeyHasherType;
    //    FieldType mSuffixKeyFieldType;
};
}} // namespace indexlib::index

#endif //__INDEXLIB_KKV_READER_H
