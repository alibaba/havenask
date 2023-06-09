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
#ifndef __INDEXLIB_CODEGEN_KKV_READER_H
#define __INDEXLIB_CODEGEN_KKV_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class CodegenKKVReader final : public KKVReader
{
public:
    CodegenKKVReader();
    ~CodegenKKVReader();
    CodegenKKVReader(const CodegenKKVReader& rhs);

public:
    typedef KKVReaderImplBase KKVReaderImplType;
    DEFINE_SHARED_PTR(KKVReaderImplType);

public:
    void Open(const config::KKVIndexConfigPtr& kkvConfig, const index_base::PartitionDataPtr& partitionData,
              const util::SearchCachePartitionWrapperPtr& searchCache, int64_t latestNeedSkipIncTs,
              const std::string& schemaName = "") override;

    // for single multi-region kkv reader
    void ReInit(const config::KKVIndexConfigPtr& kkvConfig, const index_base::PartitionDataPtr& partitionData,
                int64_t latestNeedSkipIncTs) override;

    KKVReader* Clone() override
    {
        KKVReader* ret = new CodegenKKVReader(*this);
        return ret;
    }

    KKVReaderPtr CreateCodegenKKVReader() override;

    FL_LAZY(KKVIterator*)
    LookupAsync(PKeyType pkeyHash, std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>> suffixKeyHashVec,
                uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default,
                autil::mem_pool::Pool* pool = nullptr, KVMetricsCollector* metricsCollector = nullptr) override;

    FL_LAZY(KKVIterator*)
    LookupAsync(PKeyType pkeyHash, std::vector<uint64_t> suffixKeyHashVec, uint64_t timestamp,
                TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = nullptr,
                KVMetricsCollector* metricsCollector = nullptr) override;

    FL_LAZY(KKVIterator*)
    LookupAsync(const autil::StringView& prefixKey, uint64_t timestamp,
                TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = nullptr,
                KVMetricsCollector* metricsCollector = nullptr) override;

    FL_LAZY(KKVIterator*)
    LookupAsync(const autil::StringView& prefixKey,
                const std::vector<autil::StringView, autil::mem_pool::pool_allocator<autil::StringView>>& suffixKeys,
                uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default,
                autil::mem_pool::Pool* pool = nullptr, KVMetricsCollector* metricsCollector = nullptr) override
    {
        FL_CORETURN FL_COAWAIT InnerLookupAsync(prefixKey, suffixKeys, timestamp, searchCacheType, pool,
                                                metricsCollector);
    }

    FL_LAZY(KKVIterator*)
    LookupAsync(const autil::StringView& prefixKey, const std::vector<autil::StringView>& suffixKeys,
                uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default,
                autil::mem_pool::Pool* pool = nullptr, KVMetricsCollector* metricsCollector = nullptr) override
    {
        FL_CORETURN FL_COAWAIT InnerLookupAsync(prefixKey, suffixKeys, timestamp, searchCacheType, pool,
                                                metricsCollector);
    }

    FL_LAZY(KKVIterator*)
    LookupAsync(PKeyType pkeyHash,
                const std::vector<autil::StringView, autil::mem_pool::pool_allocator<autil::StringView>>& suffixKeys,
                uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default,
                autil::mem_pool::Pool* pool = nullptr, KVMetricsCollector* metricsCollector = nullptr) override
    {
        FL_CORETURN FL_COAWAIT InnerLookupAsync(pkeyHash, suffixKeys, timestamp, searchCacheType, pool,
                                                metricsCollector);
    }

    FL_LAZY(KKVIterator*)
    LookupAsync(PKeyType pkeyHash, const std::vector<autil::StringView>& suffixKeys, uint64_t timestamp,
                TableSearchCacheType searchCacheType = tsc_default, autil::mem_pool::Pool* pool = nullptr,
                KVMetricsCollector* metricsCollector = nullptr) override
    {
        FL_CORETURN FL_COAWAIT InnerLookupAsync(pkeyHash, suffixKeys, timestamp, searchCacheType, pool,
                                                metricsCollector);
    }

    FL_LAZY(KKVIterator*)
    LookupAsync(PKeyType pkeyHash, uint64_t timestamp, TableSearchCacheType searchCacheType = tsc_default,
                autil::mem_pool::Pool* pool = nullptr, KVMetricsCollector* metricsCollector = nullptr) override;

    void TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id) override;

private:
    template <typename Alloc>
    inline FL_LAZY(KKVIterator*)
        InnerLookupAsync(const autil::StringView& prefixKey, const std::vector<autil::StringView, Alloc>& suffixKeys,
                         uint64_t timestamp, TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool,
                         KVMetricsCollector* metricsCollector);

    template <typename Alloc>
    inline FL_LAZY(KKVIterator*)
        InnerLookupAsync(PKeyType pkeyHash, const std::vector<autil::StringView, Alloc>& suffixKeys, uint64_t timestamp,
                         TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool,
                         KVMetricsCollector* metricsCollector);

    inline bool GetPKeyHash(const autil::StringView& prefixKey, PKeyType& pkey)
    {
        dictkey_t key = 0;
        if (unlikely(!util::GetHashKey(mPrefixKeyHasherType, prefixKey, key))) {
            IE_LOG(ERROR, "hash prefix key [%s] of type[%d] failed", prefixKey.data(), mPrefixKeyHasherType);
            return false;
        }
        pkey = (PKeyType)key;
        return true;
    }

    inline bool GetSKeyHash(const autil::StringView& suffixKey, uint64_t& skey)
    {
        dictkey_t key = 0;
        if (unlikely(!util::GetHashKey(mSuffixKeyHasherType, suffixKey, key))) {
            IE_LOG(ERROR, "hash suffix key [%s] of type[%d] failed", suffixKey.data(), mSuffixKeyHasherType);
            return false;
        }
        skey = (uint64_t)key;
        return true;
    }

    template <typename inputAlloc = std::allocator<autil::StringView>, typename outputAlloc = std::allocator<uint64_t>>
    bool GetSKeyHashVec(const std::vector<autil::StringView, inputAlloc>& suffixKeys,
                        std::vector<uint64_t, outputAlloc>& skeyHashs)
    {
        assert(skeyHashs.empty());
        skeyHashs.reserve(suffixKeys.size());
        for (size_t i = 0; i < suffixKeys.size(); ++i) {
            uint64_t skey;
            if (GetSKeyHash(suffixKeys[i], skey)) {
                skeyHashs.push_back(skey);
            }
        }
        return !skeyHashs.empty() || suffixKeys.empty();
    }

protected:
    bool doCollectAllCode() override;

private:
    KKVReaderImplTypePtr mReader;
    // friend class KKVReaderTest;
    // codegen
    std::string mReaderType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CodegenKKVReader);

////////////////////////////////////////////////////////////
inline FL_LAZY(KKVIterator*) CodegenKKVReader::LookupAsync(const autil::StringView& prefixKey, uint64_t timestamp,
                                                           TableSearchCacheType searchCacheType,
                                                           autil::mem_pool::Pool* pool,
                                                           KVMetricsCollector* metricsCollector)
{
    assert(pool);
    PKeyType pkeyHash = (PKeyType)0;
    if (!GetPKeyHash(prefixKey, pkeyHash)) {
        if (metricsCollector) {
            metricsCollector->EndQuery();
        }
        FL_CORETURN nullptr;
    }
    autil::mem_pool::pool_allocator<uint64_t> alloc(pool);
    std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>> suffixKeyHashs(alloc);
    FL_CORETURN FL_COAWAIT LookupAsync(pkeyHash, std::move(suffixKeyHashs), timestamp, searchCacheType, pool,
                                       metricsCollector);
}

inline FL_LAZY(KKVIterator*) CodegenKKVReader::LookupAsync(PKeyType pkeyHash, uint64_t timestamp,
                                                           TableSearchCacheType searchCacheType,
                                                           autil::mem_pool::Pool* pool,
                                                           KVMetricsCollector* metricsCollector)
{
    assert(pool);
    autil::mem_pool::pool_allocator<int64_t> alloc(pool);
    std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>> suffixKeyHashs(alloc);
    FL_CORETURN FL_COAWAIT LookupAsync(pkeyHash, std::move(suffixKeyHashs), timestamp, searchCacheType, pool,
                                       metricsCollector);
}

template <typename Alloc>
inline FL_LAZY(KKVIterator*) CodegenKKVReader::InnerLookupAsync(
    const autil::StringView& prefixKey, const std::vector<autil::StringView, Alloc>& suffixKeys, uint64_t timestamp,
    TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool, KVMetricsCollector* metricsCollector)
{
    assert(pool);
    PKeyType pkeyHash = (PKeyType)0;
    if (!GetPKeyHash(prefixKey, pkeyHash)) {
        if (metricsCollector) {
            metricsCollector->EndQuery();
        }
        FL_CORETURN nullptr;
    }
    FL_CORETURN FL_COAWAIT LookupAsync(pkeyHash, suffixKeys, timestamp, searchCacheType, pool, metricsCollector);
}

template <typename Alloc>
inline FL_LAZY(KKVIterator*) CodegenKKVReader::InnerLookupAsync(
    PKeyType pkeyHash, const std::vector<autil::StringView, Alloc>& suffixKeys, uint64_t timestamp,
    TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool, KVMetricsCollector* metricsCollector)
{
    assert(pool);
    autil::mem_pool::pool_allocator<uint64_t> alloc(pool);
    std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>> skeyHashs(alloc);
    if (!GetSKeyHashVec(suffixKeys, skeyHashs)) {
        if (metricsCollector) {
            metricsCollector->EndQuery();
        }
        FL_CORETURN nullptr;
    }
    FL_CORETURN FL_COAWAIT LookupAsync(pkeyHash, std::move(skeyHashs), timestamp, searchCacheType, pool,
                                       metricsCollector);
}

inline FL_LAZY(KKVIterator*) CodegenKKVReader::LookupAsync(PKeyType pkeyHash, std::vector<uint64_t> suffixKeyHashVec,
                                                           uint64_t timestamp, TableSearchCacheType searchCacheType,
                                                           autil::mem_pool::Pool* pool,
                                                           KVMetricsCollector* metricsCollector)
{
    assert(pool);
    FL_CORETURN FL_COAWAIT mReader->Lookup(&mKKVIndexOptions, pkeyHash, std::move(suffixKeyHashVec), timestamp,
                                           searchCacheType, pool, metricsCollector);
}

inline FL_LAZY(KKVIterator*) CodegenKKVReader::LookupAsync(
    PKeyType pkeyHash, std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>> suffixKeyHashVec,
    uint64_t timestamp, TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool,
    KVMetricsCollector* metricsCollector)
{
    assert(pool);
    FL_CORETURN FL_COAWAIT mReader->Lookup(&mKKVIndexOptions, pkeyHash, std::move(suffixKeyHashVec), timestamp,
                                           searchCacheType, pool, metricsCollector);
}
}} // namespace indexlib::index

#endif //__INDEXLIB_CODEGEN_KKV_READER_H
