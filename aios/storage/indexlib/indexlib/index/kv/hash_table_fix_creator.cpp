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
#include "indexlib/index/kv/hash_table_fix_creator.h"

#include "indexlib/common/hash_table/hash_table_file_reader_base.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_factory.h"
#include "indexlib/index/kv/kv_typeid.h"

using namespace std;

using namespace indexlib::common;

namespace indexlib { namespace index {

template <typename KeyType, typename ValueType, bool hasSpecialKey, bool compactBucket>
common::HashTableInfo InnerCreateTable(common::HashTableAccessType tableType, common::KVMap& nameInfo);

HashTableInfo HashTableFixCreator::CreateHashTableForReader(const config::KVIndexConfigPtr& kvIndexConfig,
                                                            bool useFileReader, bool useCompactBucket, bool isRtSegment,
                                                            KVMap& nameInfo)
{
    config::KVIndexPreference preference = kvIndexConfig->GetIndexPreference();
    std::string hashTypeStr = preference.GetHashDictParam().GetHashType();
    HashTableAccessType tt = DENSE_TABLE;
    if (isRtSegment || hashTypeStr == "dense") {
        tt = DENSE_TABLE;
        if (useFileReader) {
            tt = DENSE_READER;
        }
    } else if (hashTypeStr == "cuckoo") {
        tt = CUCKOO_TABLE;
        if (useFileReader) {
            tt = CUCKOO_READER;
        }
    } else {
        assert(false);
        return {};
    }

    bool compactHashKey = kvIndexConfig->IsCompactHashKey();
    FieldType fieldType = GetFieldType(kvIndexConfig);
    bool hasTTL = (kvIndexConfig->TTLEnabled() || isRtSegment);
    if (useCompactBucket) {
        return InnerCreateWithCompactBucket<true>(tt, compactHashKey, hasTTL, fieldType, nameInfo);
    }
    return InnerCreateWithCompactBucket<false>(tt, compactHashKey, hasTTL, fieldType, nameInfo);
}

HashTableInfo HashTableFixCreator::CreateHashTableForWriter(const config::KVIndexConfigPtr& kvIndexConfig,
                                                            const KVFormatOptionsPtr& kvOptions, bool isOnline)
{
    KVTypeId kvTypeId = KVFactory::GetKVTypeId(kvIndexConfig, kvOptions);
    assert(kvTypeId.onlineIndexType != indexlibv2::index::KVIndexType::KIT_CUCKOO_HASH);
    HashTableAccessType tt = DENSE_TABLE;
    decltype(kvTypeId.onlineIndexType) indexType = isOnline ? kvTypeId.onlineIndexType : kvTypeId.offlineIndexType;
    if (indexType == indexlibv2::index::KVIndexType::KIT_DENSE_HASH) {
        tt = DENSE_TABLE;
    } else if (indexType == indexlibv2::index::KVIndexType::KIT_CUCKOO_HASH) {
        tt = CUCKOO_TABLE;
    } else {
        assert(false);
    }
    bool compactHashKey = kvIndexConfig->IsCompactHashKey();
    bool hasTTL = isOnline || kvTypeId.hasTTL;
    FieldType fieldType = GetFieldType(kvIndexConfig);
    KVMap nameInfo;
    if (kvTypeId.useCompactBucket) {
        return InnerCreateWithCompactBucket<true>(tt, compactHashKey, hasTTL, fieldType, nameInfo);
    } else {
        return InnerCreateWithCompactBucket<false>(tt, compactHashKey, hasTTL, fieldType, nameInfo);
    }
}

FieldType HashTableFixCreator::GetFieldType(const config::KVIndexConfigPtr& kvIndexConfig)
{
    auto valueConfig = kvIndexConfig->GetValueConfig();
    FieldType fieldType;
    if (valueConfig->IsCompactFormatEnabled()) {
        // assert(kvIndexConfig->GetIndexPreference().GetValueParam().IsFixLenAutoInline());
        fieldType = valueConfig->GetFixedLengthValueType();
    } else {
        // legacy code
        assert(kvIndexConfig->GetRegionCount() == 1 && valueConfig->GetAttributeCount() == 1);
        config::AttributeConfigPtr attrConfig = valueConfig->GetAttributeConfig(0);
        fieldType = attrConfig->GetFieldType();
    }
    return fieldType;
}

template <bool compactBucket>
HashTableInfo HashTableFixCreator::InnerCreateWithCompactBucket(HashTableAccessType tableType, bool compactHashKey,
                                                                bool hasTTL, FieldType fieldType, KVMap& nameInfo)
{
    if (compactHashKey) {
        nameInfo["CompactBucket"] = "true";
        nameInfo["KeyType"] = "compact_keytype_t";
        return InnerCreateWithKeyType<compactBucket, compact_keytype_t>(tableType, hasTTL, fieldType, nameInfo);
    }
    nameInfo["CompactBucket"] = "false";
    nameInfo["KeyType"] = "keytype_t";
    return InnerCreateWithKeyType<compactBucket, keytype_t>(tableType, hasTTL, fieldType, nameInfo);
}

#define HASHTABLEFIXCREATOR_EXTERN_TEMPLATE_INTERNAL_DECL_IMPL(FieldType, hasSpecialKey)                               \
    extern template HashTableInfo InnerCreateTable<keytype_t, FieldType, hasSpecialKey, true>(                         \
        HashTableAccessType tableType, KVMap & nameInfo);                                                              \
    extern template HashTableInfo InnerCreateTable<keytype_t, FieldType, hasSpecialKey, false>(                        \
        HashTableAccessType tableType, KVMap & nameInfo);                                                              \
    extern template HashTableInfo InnerCreateTable<compact_keytype_t, FieldType, hasSpecialKey, true>(                 \
        HashTableAccessType tableType, KVMap & nameInfo);                                                              \
    extern template HashTableInfo InnerCreateTable<compact_keytype_t, FieldType, hasSpecialKey, false>(                \
        HashTableAccessType tableType, KVMap & nameInfo);

#define HASHTABLEFIXCREATOR_EXTERN_TEMPLATE_INTERNAL_DECL(FieldType)                                                   \
    HASHTABLEFIXCREATOR_EXTERN_TEMPLATE_INTERNAL_DECL_IMPL(                                                            \
        common::TimestampValue<config::FieldTypeTraits<FieldType>::AttrItemType>, false)                               \
    HASHTABLEFIXCREATOR_EXTERN_TEMPLATE_INTERNAL_DECL_IMPL(                                                            \
        common::Timestamp0Value<config::FieldTypeTraits<FieldType>::AttrItemType>, true)

FIX_LEN_FIELD_MACRO_HELPER(HASHTABLEFIXCREATOR_EXTERN_TEMPLATE_INTERNAL_DECL)

template <bool compactBucket, typename KeyType>
HashTableInfo HashTableFixCreator::InnerCreateWithKeyType(HashTableAccessType tableType, bool hasTTL,
                                                          FieldType fieldType, KVMap& nameInfo)
{
    std::stringstream ss;
    switch (fieldType) {
#define MACRO(fieldType)                                                                                               \
    case fieldType: {                                                                                                  \
        typedef config::FieldTypeTraits<fieldType>::AttrItemType T;                                                    \
        std::string fieldTypeName = config::FieldTypeTraits<fieldType>::GetTypeName();                                 \
        if (hasTTL) {                                                                                                  \
            ss << "common::TimestampValue<" << fieldTypeName << ">";                                                   \
            nameInfo["ValueType"] = ss.str();                                                                          \
            nameInfo["HasSpecialKey"] = "false";                                                                       \
            typedef common::TimestampValue<T> ValueType;                                                               \
            return InnerCreateTable<KeyType, ValueType, false, compactBucket>(tableType, nameInfo);                    \
        }                                                                                                              \
        typedef common::Timestamp0Value<T> ValueType;                                                                  \
        ss << "common::Timestamp0Value<" << fieldTypeName << ">";                                                      \
        nameInfo["ValueType"] = ss.str();                                                                              \
        nameInfo["HasSpecialKey"] = "true";                                                                            \
        return InnerCreateTable<KeyType, ValueType, true, compactBucket>(tableType, nameInfo);                         \
    }
        FIX_LEN_FIELD_MACRO_HELPER(MACRO);
#undef MACRO
    default: {
        assert(false);
        return {};
    }
    }
    return {};
}
}} // namespace indexlib::index
