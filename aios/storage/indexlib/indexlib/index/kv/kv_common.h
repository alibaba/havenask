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
#ifndef __INDEXLIB_KV_COMMON_H
#define __INDEXLIB_KV_COMMON_H

#include "indexlib/common/hash_table/cuckoo_hash_table_traits.h"
#include "indexlib/common/hash_table/dense_hash_table_traits.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/kv_define.h"

namespace indexlib { namespace index {

// HashTable Traits
template <indexlibv2::index::KVIndexType Type, typename KeyType, typename ValueType, bool useCompactBucket>
struct HashTableTraits;

template <typename KeyType, typename ValueType, bool useCompactBucket>
struct HashTableTraits<indexlibv2::index::KVIndexType::KIT_DENSE_HASH, KeyType, ValueType, useCompactBucket> {
    typedef common::DenseHashTableTraits<KeyType, ValueType, useCompactBucket> Traits;
};

template <typename KeyType, typename ValueType, bool useCompactBucket>
struct HashTableTraits<indexlibv2::index::KVIndexType::KIT_CUCKOO_HASH, KeyType, ValueType, useCompactBucket> {
    typedef common::CuckooHashTableTraits<KeyType, ValueType, useCompactBucket> Traits;
};

struct KVVarOffsetFormatterBase {
    virtual ~KVVarOffsetFormatterBase() {};

public:
    virtual regionid_t GetRegionId(offset_t offset, const regionid_t* regionId = nullptr) const = 0;
    virtual offset_t GetOffset(offset_t offset, const regionid_t* regionId = nullptr) const = 0;
    virtual offset_t GetValue(offset_t offset, const regionid_t* regionId = nullptr) const = 0;
};

template <typename OffsetValue>
struct KVVarOffsetFormatter final : public KVVarOffsetFormatterBase {
public:
    regionid_t GetRegionId(offset_t offset, const regionid_t* regionId = nullptr) const override
    {
        assert(regionId == nullptr || *regionId == DEFAULT_REGIONID);
        return DEFAULT_REGIONID;
    }
    offset_t GetOffset(offset_t offset, const regionid_t* regionId = nullptr) const override
    {
        assert(regionId == nullptr || *regionId == DEFAULT_REGIONID);
        return offset;
    }
    offset_t GetValue(offset_t offset, const regionid_t* regionId = nullptr) const override
    {
        assert(regionId == nullptr || *regionId == DEFAULT_REGIONID);
        return offset;
    }
};

#define DECLARE_OFFSET_FORMATTER_IMPL(ValueType)                                                                       \
    template <>                                                                                                        \
    struct KVVarOffsetFormatter<ValueType> final : public KVVarOffsetFormatterBase {                                   \
    public:                                                                                                            \
        regionid_t GetRegionId(offset_t offset, const regionid_t* regionId = nullptr) const override                   \
        {                                                                                                              \
            if (regionId) {                                                                                            \
                return MultiRegionOffsetFormatter(offset, *regionId).GetRegionId();                                    \
            } else {                                                                                                   \
                return MultiRegionOffsetFormatter(offset).GetRegionId();                                               \
            }                                                                                                          \
        }                                                                                                              \
        offset_t GetOffset(offset_t offset, const regionid_t* regionId = nullptr) const override                       \
        {                                                                                                              \
            if (regionId) {                                                                                            \
                return MultiRegionOffsetFormatter(offset, *regionId).GetOffset();                                      \
            } else {                                                                                                   \
                return MultiRegionOffsetFormatter(offset).GetOffset();                                                 \
            }                                                                                                          \
        }                                                                                                              \
        offset_t GetValue(offset_t offset, const regionid_t* regionId = nullptr) const override                        \
        {                                                                                                              \
            if (regionId) {                                                                                            \
                return MultiRegionOffsetFormatter(offset, *regionId).GetValue();                                       \
            } else {                                                                                                   \
                return MultiRegionOffsetFormatter(offset).GetValue();                                                  \
            }                                                                                                          \
        }                                                                                                              \
    };

DECLARE_OFFSET_FORMATTER_IMPL(MultiRegionTimestamp0Value<offset_t>);
DECLARE_OFFSET_FORMATTER_IMPL(MultiRegionTimestampValue<offset_t>);

#undef DECLARE_OFFSET_FORMATTER_IMPL
}} // namespace indexlib::index

#endif //__INDEXLIB_KV_COMMON_H
