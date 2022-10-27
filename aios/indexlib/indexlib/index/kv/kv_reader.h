#ifndef __INDEXLIB_KV_READER_H
#define __INDEXLIB_KV_READER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_hasher_typed.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_read_options.h"
#include "indexlib/index/kv/kv_reader_impl_base.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/index/normal/attribute/attribute_value_type_traits.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);

IE_NAMESPACE_BEGIN(index);

class KVReader
{
public:
    KVReader();
    ~KVReader();

public:
    void Open(const config::KVIndexConfigPtr& kvConfig,
              const index_base::PartitionDataPtr& partitionData,
              const util::SearchCachePartitionWrapperPtr& searchCache,
              int64_t latestIncSkipTs)
    {
        assert(false);
    }

    // for single multi-region kkv reader
    void Open(const config::KVIndexConfigPtr& kvConfig,
              const index_base::PartitionDataPtr& partitionData,
              const KVReaderImplBasePtr& reader,
              int64_t latestNeedSkipIncTs)
    {
        assert(false);
    }

    bool Get(const autil::ConstString& key,
             autil::ConstString& value,
             uint64_t ts = 0,
             TableSearchCacheType searchCacheType = tsc_default,
             autil::mem_pool::Pool* pool = NULL,
             KVMetricsCollector* metricsCollector = NULL) const
    {
        assert(false);
        return false;
    }

    bool Get(keytype_t keyHash,
             autil::ConstString& value,
             uint64_t ts = 0,
             TableSearchCacheType searchCacheType = tsc_default,
             autil::mem_pool::Pool* pool = NULL,
             KVMetricsCollector* metricsCollector = NULL) const
    {
        assert(false);
        return false;
    }

    template<typename T>
    bool Get(const autil::ConstString& key, T& value,
             const KVReadOptions& options) const
    {
        assert(false);
        return false;
    }

    template<typename T>
    bool Get(keytype_t keyHash, T& value,
             const KVReadOptions& options) const
    {
        assert(false);
        return false;
    }

    const config::KVIndexConfigPtr& GetKVIndexConfig() const
    {
        assert(false);
        return config::KVIndexConfigPtr();
    }
    KVValueType GetValueType() const
    {
        assert(false);
        return KVVT_TYPED;
    }

    bool GetHashKey(const autil::ConstString& keyStr, dictkey_t &key) const
    {
        assert(false);
        return false;
    }

    const KVReaderImplBasePtr& GetReaderImpl() const
    {
        assert(false);
        return KVReaderImplBasePtr();
    }

    const common::PackAttributeFormatterPtr& GetPackAttributeFormatter() const
    {
        assert(false);
        return common::PackAttributeFormatterPtr();
    }

    attrid_t GetAttributeId(const std::string& fieldName) const
    {
        assert(false);
        return INVALID_ATTRID;
    }

    HashFunctionType GetHasherType() const
    {
        assert(false);
        return hft_unknown;
    }

    static bool GetHashKeyWithType(HashFunctionType hashType,
                                   const autil::ConstString& keyStr, dictkey_t &key)
    {
        assert(false);
        return false;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_KV_READER_H
