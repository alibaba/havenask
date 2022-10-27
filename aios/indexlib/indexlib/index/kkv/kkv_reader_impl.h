#ifndef __INDEX_KKV_READER_IMPL_H
#define __INDEX_KKV_READER_IMPL_H

#include <tr1/memory>
#include <tr1/unordered_set>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/util/cache/search_cache_partition_wrapper.h"

IE_NAMESPACE_BEGIN(index);

class KKVReaderImplBase
{
public:
    KKVReaderImplBase()
    {}

    virtual ~KKVReaderImplBase() {}

public:
    virtual void Open(const config::KKVIndexConfigPtr& kkvConfig,
                      const index_base::PartitionDataPtr& partitionData,
                      const util::SearchCachePartitionWrapperPtr& searchCache)
    {
        assert(false);
    }
};

DEFINE_SHARED_PTR(KKVReaderImplBase);

IE_NAMESPACE_END(index);

#endif //__INDEX_KKV_READER_IMPL_H
