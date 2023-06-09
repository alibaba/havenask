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
#include "indexlib/index/kv/VarLenHashTableCreator.h"

#include "autil/Log.h"
#include "indexlib/index/common/hash_table/BucketOffsetCompressor.h"
#include "indexlib/index/common/hash_table/ClosedHashTableFileIterator.h"
#include "indexlib/index/common/hash_table/CuckooHashTable.h"
#include "indexlib/index/common/hash_table/CuckooHashTableFileReader.h"
#include "indexlib/index/common/hash_table/DenseHashTable.h"
#include "indexlib/index/common/hash_table/DenseHashTableFileReader.h"
#include "indexlib/index/kv/KVTypeId.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, VarLenHashTableCreator);

using HashTableInfoPtr = typename VarLenHashTableCreator::HashTableInfoPtr;

HashTableInfoPtr VarLenHashTableCreator::CreateHashTableForReader(const KVTypeId& typeId, bool useFileReader)
{
    assert(typeId.isVarLen);
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

HashTableInfoPtr VarLenHashTableCreator::CreateHashTableForWriter(const KVTypeId& typeId)
{
    assert(typeId.isVarLen);
    if (typeId.offlineIndexType == KVIndexType::KIT_DENSE_HASH) {
        return InnerCreate(DENSE_TABLE, typeId);
    } else if (typeId.offlineIndexType == KVIndexType::KIT_CUCKOO_HASH) {
        return InnerCreate(CUCKOO_TABLE, typeId);
    } else {
        return nullptr;
    }
}

HashTableInfoPtr VarLenHashTableCreator::CreateHashTableForMerger(const KVTypeId& typeId)
{
    assert(typeId.isVarLen);
    if (typeId.offlineIndexType == KVIndexType::KIT_DENSE_HASH) {
        return InnerCreate(DENSE_READER, typeId);
    } else if (typeId.offlineIndexType == KVIndexType::KIT_CUCKOO_HASH) {
        return InnerCreate(CUCKOO_READER, typeId);
    } else {
        return nullptr;
    }
}

HashTableInfoPtr VarLenHashTableCreator::InnerCreate(HashTableAccessType accessType, const KVTypeId& typeId)
{
    if (typeId.compactHashKey) {
        return CreateWithKeyType<compact_keytype_t>(accessType, typeId);
    } else {
        return CreateWithKeyType<keytype_t>(accessType, typeId);
    }
}

template <typename KeyType>
HashTableInfoPtr VarLenHashTableCreator::CreateWithKeyType(HashTableAccessType accessType, const KVTypeId& typeId)
{
    if (typeId.shortOffset) {
        if (typeId.hasTTL) {
            using ValueType = TimestampValue<short_offset_t>;
            return CreateWithKeyTypeValueType<KeyType, ValueType>(accessType, typeId);
        } else {
            using ValueType = OffsetValue<short_offset_t>;
            return CreateWithKeyTypeValueType<KeyType, ValueType>(accessType, typeId);
        }
    } else {
        if (typeId.hasTTL) {
            using ValueType = TimestampValue<offset_t>;
            return CreateWithKeyTypeValueType<KeyType, ValueType>(accessType, typeId);
        } else {
            using ValueType = OffsetValue<offset_t>;
            return CreateWithKeyTypeValueType<KeyType, ValueType>(accessType, typeId);
        }
    }
}

template <typename KeyType, typename ValueType>
HashTableInfoPtr VarLenHashTableCreator::CreateWithKeyTypeValueType(HashTableAccessType accessType,
                                                                    const KVTypeId& typeId)
{
    auto hashTableInfo = std::make_unique<HashTableInfo>();
    switch (accessType) {
    case DENSE_TABLE: {
        using HashTableType = DenseHashTable<KeyType, ValueType, false>;
        hashTableInfo->hashTable = std::make_unique<HashTableType>();
        using BucketCompressorType = BucketOffsetCompressor<typename HashTableType::Bucket>;
        hashTableInfo->bucketCompressor = std::make_unique<BucketCompressorType>();
        break;
    }
    case DENSE_READER: {
        using HashTableType = DenseHashTableFileReader<KeyType, ValueType, false>;
        using IteratorType = ClosedHashTableFileIterator<typename HashTableType::HashTableHeader, KeyType, ValueType,
                                                         false, true, false>;
        hashTableInfo->hashTable = std::make_unique<HashTableType>();
        hashTableInfo->hashTableFileIterator = std::make_unique<IteratorType>();
        break;
    }
    case CUCKOO_TABLE: {
        using HashTableType = CuckooHashTable<KeyType, ValueType, false>;
        hashTableInfo->hashTable = std::make_unique<HashTableType>();
        using BucketCompressorType = BucketOffsetCompressor<typename HashTableType::Bucket>;
        hashTableInfo->bucketCompressor = std::make_unique<BucketCompressorType>();
        break;
    }
    case CUCKOO_READER: {
        using HashTableType = CuckooHashTableFileReader<KeyType, ValueType, false>;
        using IteratorType = ClosedHashTableFileIterator<typename HashTableType::HashTableHeader, KeyType, ValueType,
                                                         false, true, false>;
        hashTableInfo->hashTable = std::make_unique<HashTableType>();
        hashTableInfo->hashTableFileIterator = std::make_unique<IteratorType>();
        break;
    }
    default:
        return nullptr;
    }
    hashTableInfo->valueUnpacker.reset(ValueType::CreateValueUnpacker());
    return hashTableInfo;
}

} // namespace indexlibv2::index
