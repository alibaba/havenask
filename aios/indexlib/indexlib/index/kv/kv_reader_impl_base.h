#ifndef __INDEXLIB_KV_READER_IMPL_BASE_H
#define __INDEXLIB_KV_READER_IMPL_BASE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kv/kv_define.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, KVIndexConfig);
DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);

IE_NAMESPACE_BEGIN(index);

class KVMetricsCollector;
class KVIndexOptions;

class KVReaderImplBase
{
public:
    KVReaderImplBase() {}
    virtual ~KVReaderImplBase() {}
public:
    virtual void Open(const config::KVIndexConfigPtr& kvIndexConfig,
                      const index_base::PartitionDataPtr& partitionData,
                      const util::SearchCachePartitionWrapperPtr& searchCache)
    {
        assert(false);
    }

    virtual bool Get(const KVIndexOptions* options, keytype_t key,
                     autil::ConstString& value, uint64_t ts = 0,
                     TableSearchCacheType searchCacheType = tsc_default,
                     autil::mem_pool::Pool* pool = NULL,
                     KVMetricsCollector* metricsCollector = NULL) const
    {
        assert(false);
        return false;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVReaderImplBase);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_KV_READER_IMPL_BASE_H
