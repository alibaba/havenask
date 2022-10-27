#ifndef __INDEXLIB_KV_DEFINE_H
#define __INDEXLIB_KV_DEFINE_H

#include <string>
#include <indexlib/common/hash_table/closed_hash_table_traits.h>
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/index/kv/multi_region_special_value.h"

IE_NAMESPACE_BEGIN(index);

const std::string KV_KEY_COUNT = "key_count";
const std::string KV_SEGMENT_MEM_USE = "kv_segment_mem_use";
const std::string KV_HASH_MEM_USE = "kv_hash_mem_use";
const std::string KV_HASH_OCCUPANCY_PCT = "kv_hash_occupancy_pct";
const std::string KV_VALUE_MEM_USE = "kv_value_mem_use";
const std::string KV_KEY_VALUE_MEM_RATIO = "kv_key_value_mem_ratio";
const std::string KV_WRITER_TYPEID = "kv_writer_typeid";
const std::string KV_KEY_DELETE_COUNT = "kv_key_delete_count";

const std::string KV_COMPRESS_RATIO_GROUP_NAME = "kv_compress_ratio";

typedef uint64_t keytype_t;
typedef uint32_t compact_keytype_t;
typedef uint64_t offset_t;
typedef uint32_t short_offset_t;

enum KVValueType
{
    KVVT_TYPED,
    KVVT_PACKED_ONE_FIELD,
    KVVT_PACKED_MULTI_FIELD,
};

enum KVIndexType
{
    KIT_DENSE_HASH,
    KIT_CUCKOO_HASH,
    KIT_UNKOWN
};

// KeyType Traits
template<bool compactKey>
struct HashKeyTypeTraits { typedef keytype_t HashKeyType; };
template<>
struct HashKeyTypeTraits<true> { typedef compact_keytype_t HashKeyType; };

template<bool shortOffset>
struct HashOffsetTypeTraits { typedef offset_t HashOffsetType; };
template<>
struct HashOffsetTypeTraits<true> { typedef short_offset_t HashOffsetType; };
   
// ValueType Traits for fix kv
template<typename T, bool hasTs>
struct KVValueTypeTraits { typedef common::Timestamp0Value<T> ValueType; };

template<typename T>
struct KVValueTypeTraits<T, true> { typedef common::TimestampValue<T> ValueType; };

// ValueType Traits for var kv
template<typename T, bool hasTs>
struct VarKVValueTypeTraits
{
    typedef common::OffsetValue<
        T, std::numeric_limits<T>::max(),
        std::numeric_limits<T>::max() - 1> ValueType;
};

template<typename T>
struct VarKVValueTypeTraits<T, true>
{
    typedef common::TimestampValue<T> ValueType;
};

// ValueType Traits for multi region kv
template<typename T, bool hasTs>
struct MultiRegionVarKVValueTypeTraits
{
    typedef MultiRegionTimestamp0Value<T> ValueType;
};

template<typename T>
struct MultiRegionVarKVValueTypeTraits<T, true>
{
    typedef MultiRegionTimestampValue<T> ValueType;
};

inline config::KVIndexConfigPtr CreateKVIndexConfigForMultiRegionData(
        const config::IndexPartitionSchemaPtr& schema)
{
    assert(schema->GetRegionCount() > 1);
    const config::IndexSchemaPtr& indexSchema = schema->GetIndexSchema(DEFAULT_REGIONID);
    config::KVIndexConfigPtr firstRegionKVConfig = DYNAMIC_POINTER_CAST(
            config::KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    config::IndexConfigPtr config(firstRegionKVConfig->Clone());
    config::KVIndexConfigPtr dataKVConfig = DYNAMIC_POINTER_CAST(config::KVIndexConfig, config);
    assert(dataKVConfig);

    // disable compact pack value
    const config::ValueConfigPtr& valueConfig = dataKVConfig->GetValueConfig();
    config::ValueConfigPtr cloneValueConfig(new config::ValueConfig(*valueConfig));
    cloneValueConfig->EnableCompactFormat(false);
    dataKVConfig->SetValueConfig(cloneValueConfig);

    // disable compact hash key & short offset
    config::KVIndexPreference& preference = dataKVConfig->GetIndexPreference();
    config::KVIndexPreference::HashDictParam dictParam = preference.GetHashDictParam();
    dictParam.SetEnableCompactHashKey(false);
    dictParam.SetEnableShortenOffset(false);
    preference.SetHashDictParam(dictParam);

    if (dataKVConfig->TTLEnabled())
    {
        return dataKVConfig;
    }
    for (regionid_t id = 1; id < (regionid_t)schema->GetRegionCount(); id++)
    {
        if (schema->GetRegionSchema(id)->TTLEnabled())
        {
            dataKVConfig->SetTTL(DEFAULT_TIME_TO_LIVE);
            return dataKVConfig;
        }
    }
    dataKVConfig->DisableTTL();
    return dataKVConfig;
}

inline config::KVIndexConfigPtr CreateDataKVIndexConfig(
        const config::IndexPartitionSchemaPtr& schema)
{
    if (schema->GetRegionCount() > 1)
    {
        return CreateKVIndexConfigForMultiRegionData(schema);
    }
    return DYNAMIC_POINTER_CAST(
            config::KVIndexConfig,
            schema->GetIndexSchema(DEFAULT_REGIONID)->GetPrimaryKeyIndexConfig());
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_KV_DEFINE_H
