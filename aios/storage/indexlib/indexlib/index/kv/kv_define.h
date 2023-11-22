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

#include <string>

#include "future_lite/CoroInterface.h"
#include "indexlib/common/hash_table/closed_hash_table_traits.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/index/kv/multi_region_special_value.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

extern const std::string KV_KEY_COUNT;
extern const std::string KV_SEGMENT_MEM_USE;
extern const std::string KV_HASH_MEM_USE;
extern const std::string KV_HASH_OCCUPANCY_PCT;
extern const std::string KV_VALUE_MEM_USE;
extern const std::string KV_KEY_VALUE_MEM_RATIO;
extern const std::string KV_WRITER_TYPEID;
extern const std::string KV_KEY_DELETE_COUNT;

extern const std::string KV_COMPRESS_RATIO_GROUP_NAME;

typedef uint64_t keytype_t;
typedef uint32_t compact_keytype_t;

// KeyType Traits
template <bool compactKey>
struct HashKeyTypeTraits {
    typedef keytype_t HashKeyType;
};
template <>
struct HashKeyTypeTraits<true> {
    typedef compact_keytype_t HashKeyType;
};

template <bool shortOffset>
struct HashOffsetTypeTraits {
    typedef offset_t HashOffsetType;
};
template <>
struct HashOffsetTypeTraits<true> {
    typedef short_offset_t HashOffsetType;
};

// ValueType Traits for fix kv
template <typename T, bool hasTs>
struct KVValueTypeTraits {
    typedef common::Timestamp0Value<T> ValueType;
};

template <typename T>
struct KVValueTypeTraits<T, true> {
    typedef common::TimestampValue<T> ValueType;
};

// ValueType Traits for var kv
template <typename T, bool hasTs>
struct VarKVValueTypeTraits {
    typedef common::OffsetValue<T, std::numeric_limits<T>::max(), std::numeric_limits<T>::max() - 1> ValueType;
};

template <typename T>
struct VarKVValueTypeTraits<T, true> {
    typedef common::TimestampValue<T> ValueType;
};

// ValueType Traits for multi region kv
template <typename T, bool hasTs>
struct MultiRegionVarKVValueTypeTraits {
    typedef MultiRegionTimestamp0Value<T> ValueType;
};

template <typename T>
struct MultiRegionVarKVValueTypeTraits<T, true> {
    typedef MultiRegionTimestampValue<T> ValueType;
};

inline config::KVIndexConfigPtr CreateKVIndexConfigForMultiRegionData(const config::IndexPartitionSchemaPtr& schema)
{
    assert(schema->GetRegionCount() > 1);
    const config::IndexSchemaPtr& indexSchema = schema->GetIndexSchema(DEFAULT_REGIONID);
    config::KVIndexConfigPtr firstRegionKVConfig =
        DYNAMIC_POINTER_CAST(config::KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    config::IndexConfigPtr config(firstRegionKVConfig->Clone());
    config::KVIndexConfigPtr dataKVConfig = DYNAMIC_POINTER_CAST(config::KVIndexConfig, config);
    assert(dataKVConfig);

    // disable compact pack value & disable plain format
    const config::ValueConfigPtr& valueConfig = dataKVConfig->GetValueConfig();
    config::ValueConfigPtr cloneValueConfig(new config::ValueConfig(*valueConfig));
    cloneValueConfig->EnableCompactFormat(false);
    cloneValueConfig->EnablePlainFormat(false);
    dataKVConfig->SetValueConfig(cloneValueConfig);

    // disable compact hash key & short offset
    config::KVIndexPreference& preference = dataKVConfig->GetIndexPreference();
    config::KVIndexPreference::HashDictParam dictParam = preference.GetHashDictParam();
    dictParam.SetEnableCompactHashKey(false);
    dictParam.SetEnableShortenOffset(false);
    preference.SetHashDictParam(dictParam);

    if (dataKVConfig->TTLEnabled()) {
        return dataKVConfig;
    }
    for (regionid_t id = 1; id < (regionid_t)schema->GetRegionCount(); id++) {
        if (schema->GetRegionSchema(id)->TTLEnabled()) {
            dataKVConfig->SetTTL(DEFAULT_TIME_TO_LIVE);
            return dataKVConfig;
        }
    }
    dataKVConfig->SetTTL(INVALID_TTL);
    return dataKVConfig;
}

inline config::KVIndexConfigPtr CreateDataKVIndexConfig(const config::IndexPartitionSchemaPtr& schema)
{
    if (schema->GetRegionCount() > 1) {
        return CreateKVIndexConfigForMultiRegionData(schema);
    }
    return DYNAMIC_POINTER_CAST(config::KVIndexConfig,
                                schema->GetIndexSchema(DEFAULT_REGIONID)->GetPrimaryKeyIndexConfig());
}

inline bool IsVarLenHashTable(const config::KVIndexConfigPtr& kvConfig)
{
    if (kvConfig->GetRegionCount() > 1) {
        return true;
    }
    auto& valueConfig = kvConfig->GetValueConfig();
    if (kvConfig->GetIndexPreference().GetValueParam().IsFixLenAutoInline()) {
        int32_t valueSize = valueConfig->GetFixedLength();
        if (valueSize > 8 || valueSize < 0) {
            return true;
        }
        return false;
    }
    if (valueConfig->GetAttributeCount() > 1) {
        return true;
    }
    config::AttributeConfigPtr attrConfig = valueConfig->GetAttributeConfig(0);
    FieldType fieldType = attrConfig->GetFieldType();
    return attrConfig->IsMultiValue() || fieldType == ft_string;
}
}} // namespace indexlib::index
