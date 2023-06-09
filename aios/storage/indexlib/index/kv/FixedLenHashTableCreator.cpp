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
#include "indexlib/index/kv/FixedLenHashTableCreator.h"

#include "autil/Log.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/common/hash_table/ClosedHashTableBufferedFileIterator.h"
#include "indexlib/index/common/hash_table/ClosedHashTableTraits.h"
#include "indexlib/index/common/hash_table/CuckooHashTable.h"
#include "indexlib/index/common/hash_table/CuckooHashTableFileReader.h"
#include "indexlib/index/common/hash_table/DenseHashTable.h"
#include "indexlib/index/common/hash_table/DenseHashTableFileReader.h"
#include "indexlib/index/kv/FixedLenCuckooHashTableCreator.h"
#include "indexlib/index/kv/FixedLenCuckooHashTableFileReaderCreator.h"
#include "indexlib/index/kv/FixedLenDenseHashTableCreator.h"
#include "indexlib/index/kv/FixedLenDenseHashTableFileReaderCreator.h"
#include "indexlib/index/kv/KVTypeId.h"

namespace indexlibv2::index {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, FixedLenHashTableCreator);

using HashTableInfoPtr = typename FixedLenHashTableCreator::HashTableInfoPtr;

HashTableInfoPtr FixedLenHashTableCreator::CreateHashTableForReader(const KVTypeId& typeId, bool useFileReader)
{
    assert(!typeId.isVarLen);
    if (useFileReader) {
        if (typeId.offlineIndexType == KVIndexType::KIT_DENSE_HASH) {
            return InnerCreate(DENSE_READER, typeId);
        } else if (typeId.offlineIndexType == KVIndexType::KIT_CUCKOO_HASH) {
            return InnerCreate(CUCKOO_READER, typeId);
        } else {
            return nullptr;
        }
    } else {
        if (typeId.offlineIndexType == KVIndexType::KIT_DENSE_HASH) {
            return InnerCreate(DENSE_TABLE, typeId);
        } else if (typeId.offlineIndexType == KVIndexType::KIT_CUCKOO_HASH) {
            return InnerCreate(CUCKOO_TABLE, typeId);
        } else {
            return nullptr;
        }
    }
}

HashTableInfoPtr FixedLenHashTableCreator::CreateHashTableForWriter(const KVTypeId& typeId)
{
    assert(!typeId.isVarLen);
    if (typeId.offlineIndexType == KVIndexType::KIT_DENSE_HASH) {
        return InnerCreate(DENSE_TABLE, typeId);
    } else if (typeId.offlineIndexType == KVIndexType::KIT_CUCKOO_HASH) {
        return InnerCreate(CUCKOO_TABLE, typeId);
    } else {
        return nullptr;
    }
}

HashTableInfoPtr FixedLenHashTableCreator::CreateHashTableForMerger(const KVTypeId& typeId)
{
    assert(!typeId.isVarLen);
    if (typeId.offlineIndexType == KVIndexType::KIT_DENSE_HASH) {
        return InnerCreate(DENSE_READER, typeId);
    } else if (typeId.offlineIndexType == KVIndexType::KIT_CUCKOO_HASH) {
        return InnerCreate(CUCKOO_READER, typeId);
    } else {
        return nullptr;
    }
}

HashTableInfoPtr FixedLenHashTableCreator::InnerCreate(HashTableAccessType accessType, const KVTypeId& typeId)
{
    if (typeId.compactHashKey) {
        return CreateWithKeyType<compact_keytype_t>(accessType, typeId);
    } else {
        return CreateWithKeyType<keytype_t>(accessType, typeId);
    }
}

template <typename KeyType>
HashTableInfoPtr FixedLenHashTableCreator::CreateWithKeyType(HashTableAccessType accessType, const KVTypeId& typeId)
{
    auto fieldType = static_cast<FieldType>(typeId.valueType);
#define CASE(ft)                                                                                                       \
    case ft: {                                                                                                         \
        typedef indexlib::index::FieldTypeTraits<ft>::AttrItemType T;                                                  \
        if (typeId.hasTTL) {                                                                                           \
            using ValueType = TimestampValue<T>;                                                                       \
            return CreateWithKeyTypeValueType<KeyType, ValueType>(accessType, typeId);                                 \
        } else {                                                                                                       \
            using ValueType = Timestamp0Value<T>;                                                                      \
            return CreateWithKeyTypeValueType<KeyType, ValueType>(accessType, typeId);                                 \
        }                                                                                                              \
    }

    switch (fieldType) {
        FIX_LEN_FIELD_MACRO_HELPER(CASE);
    default: {
        AUTIL_LOG(ERROR, "unexpected field type: %d", fieldType);
        return nullptr;
    }
    }
#undef CASE
}

template <typename KeyType, typename ValueType>
HashTableInfoPtr FixedLenHashTableCreator::CreateWithKeyTypeValueType(HashTableAccessType accessType,
                                                                      const KVTypeId& typeId)
{
    if (typeId.useCompactBucket) {
        return CreateWithKeyTypeValueTypeWithCompactBucket<KeyType, ValueType, true>(accessType, typeId);
    } else {
        return CreateWithKeyTypeValueTypeWithCompactBucket<KeyType, ValueType, false>(accessType, typeId);
    }
}

template <typename KeyType, typename ValueType, bool useCompactBucket>
HashTableInfoPtr FixedLenHashTableCreator::CreateWithKeyTypeValueTypeWithCompactBucket(HashTableAccessType accessType,
                                                                                       const KVTypeId& typeId)
{
    auto hashTableInfo = std::make_unique<HashTableInfo>();
    switch (accessType) {
    case DENSE_TABLE: {
        hashTableInfo->hashTable =
            FixedLenDenseHashTableCreator<KeyType, ValueType, useCompactBucket>::CreateHashTable();
        break;
    }
    case DENSE_READER: {
        hashTableInfo->hashTable =
            FixedLenDenseHashTableFileReaderCreator<KeyType, ValueType, useCompactBucket>::CreateHashTable();
        hashTableInfo->hashTableFileIterator =
            FixedLenDenseHashTableFileReaderCreator<KeyType, ValueType,
                                                    useCompactBucket>::CreateHashTableFileIterator();
        break;
    }
    case CUCKOO_TABLE: {
        hashTableInfo->hashTable =
            FixedLenCuckooHashTableCreator<KeyType, ValueType, useCompactBucket>::CreateHashTable();
        break;
    }
    case CUCKOO_READER: {
        hashTableInfo->hashTable =
            FixedLenCuckooHashTableFileReaderCreator<KeyType, ValueType, useCompactBucket>::CreateHashTable();
        hashTableInfo->hashTableFileIterator =
            FixedLenCuckooHashTableFileReaderCreator<KeyType, ValueType,
                                                     useCompactBucket>::CreateHashTableFileIterator();
        break;
    }
    default:
        return nullptr;
    }
    hashTableInfo->valueUnpacker.reset(ValueType::CreateValueUnpacker());
    return hashTableInfo;
}

} // namespace indexlibv2::index
