#ifndef __INDEXLIB_KKV_READER_H
#define __INDEXLIB_KKV_READER_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/sort_param.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/util/cache/search_cache_partition_wrapper.h"
#include "indexlib/index/kkv/kkv_iterator_impl_base.h"
#include "indexlib/index/kkv/kkv_reader_impl.h"
#include "indexlib/index/kkv/kkv_iterator.h"

#include "autil/ConstString.h"

IE_NAMESPACE_BEGIN(index);

class KVMetricsCollector;

class KKVReader
{
public:
    KKVReader() {}
    ~KKVReader() {}

public:
    void Open(const config::KKVIndexConfigPtr& kkvConfig,
              const index_base::PartitionDataPtr& partitionData,
              const util::SearchCachePartitionWrapperPtr& searchCache,
              int64_t latestNeedSkipIncTs)
    {
        assert(false);
    }

    // for single multi-region kkv reader
    void Open(const config::KKVIndexConfigPtr& kkvConfig,
              const index_base::PartitionDataPtr& partitionData,
              const KKVReaderImplBasePtr& reader,
              int64_t latestNeedSkipIncTs)
    {
        assert(false);
    }

    KKVIterator* Lookup(const autil::ConstString& prefixKey,
                        uint64_t timestamp,
                        TableSearchCacheType searchCacheType = tsc_default,
                        autil::mem_pool::Pool *pool = NULL,
                        KVMetricsCollector* metricsCollector = NULL)
    {
        assert(false);
        return nullptr;
    }


    KKVIterator* Lookup(PKeyType pkeyHash, uint64_t timestamp,
                        TableSearchCacheType searchCacheType = tsc_default,
                        autil::mem_pool::Pool *pool = NULL,
                        KVMetricsCollector* metricsCollector = NULL)
    {
        assert(false);
        return nullptr;
    }

    template<typename Alloc = std::allocator<autil::ConstString> >
    KKVIterator* Lookup(const autil::ConstString& prefixKey,
                        const std::vector<autil::ConstString, Alloc>& suffixKeys,
                        uint64_t timestamp,
                        TableSearchCacheType searchCacheType = tsc_default,
                        autil::mem_pool::Pool *pool = NULL,
                        KVMetricsCollector* metricsCollector = NULL)
    {
        assert(false);
        return nullptr;
    }

public:
    // for index printer
    KKVIterator* Lookup(PKeyType pkeyHash,
                        uint64_t timestamp,
                        autil::mem_pool::Pool *pool = NULL,
                        KVMetricsCollector* metricsCollector = NULL)
    {
        assert(false);
        return nullptr;
    }

    template<typename Alloc = std::allocator<autil::ConstString> >
    KKVIterator* Lookup(PKeyType pkeyHash,
                        const std::vector<autil::ConstString, Alloc>& suffixKeys,
                        uint64_t timestamp,
                        TableSearchCacheType searchCacheType = tsc_default,
                        autil::mem_pool::Pool *pool = NULL,
                        KVMetricsCollector* metricsCollector = NULL)
    {
        assert(false);
        return nullptr;
    }

    template<typename Alloc = std::allocator<uint64_t> >
    KKVIterator* Lookup(PKeyType pkeyHash,
                        const std::vector<uint64_t, Alloc>& suffixKeyHashVec,
                        uint64_t timestamp,
                        TableSearchCacheType searchCacheType = tsc_default,
                        autil::mem_pool::Pool *pool = NULL,
                        KVMetricsCollector* metricsCollector = NULL)
    {
        assert(false);
        return nullptr;
    }

    const config::KKVIndexConfigPtr& GetKKVIndexConfig() const
    {
        assert(false);
        return config::KKVIndexConfigPtr();
    }
    const config::SortParams& GetSortParams() const {
        assert(false);
        return config::SortParams();
    }
};

DEFINE_SHARED_PTR(KKVReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_KKV_READER_H
