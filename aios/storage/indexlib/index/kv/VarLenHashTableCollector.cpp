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
#include "indexlib/index/kv/VarLenHashTableCollector.h"

#include "autil/Log.h"
#include "indexlib/index/common/hash_table/CuckooHashTable.h"
#include "indexlib/index/common/hash_table/DenseHashTable.h"

namespace indexlibv2::index {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, VarLenHashTableCollector);

void VarLenHashTableCollector::CollectRecords(const KVTypeId& typeId, const std::shared_ptr<HashTableBase>& hashTable,
                                              const CollectFuncType& func)
{
    if (typeId.offlineIndexType == KVIndexType::KIT_DENSE_HASH) {
        InnerCollect(DENSE_TABLE, typeId, hashTable, func);
    } else if (typeId.offlineIndexType == KVIndexType::KIT_CUCKOO_HASH) {
        InnerCollect(CUCKOO_TABLE, typeId, hashTable, func);
    } else {
        AUTIL_LOG(ERROR, "offline index type [%d] is not support, can't collect records",
                  (int32_t)typeId.offlineIndexType);
    }
}

void VarLenHashTableCollector::InnerCollect(HashTableAccessType accessType, const KVTypeId& typeId,
                                            const std::shared_ptr<HashTableBase>& hashTable,
                                            const CollectFuncType& func)
{
    if (typeId.compactHashKey) {
        CollectWithKeyType<compact_keytype_t>(accessType, typeId, hashTable, func);
    } else {
        CollectWithKeyType<keytype_t>(accessType, typeId, hashTable, func);
    }
}

template <typename KeyType>
void VarLenHashTableCollector::CollectWithKeyType(HashTableAccessType accessType, const KVTypeId& typeId,
                                                  const std::shared_ptr<HashTableBase>& hashTable,
                                                  const CollectFuncType& func)
{
    if (typeId.shortOffset) {
        if (typeId.hasTTL) {
            using ValueType = TimestampValue<short_offset_t>;
            CollectWithKeyTypeValueType<KeyType, ValueType>(accessType, typeId, hashTable, func);
        } else {
            using ValueType = OffsetValue<short_offset_t>;
            CollectWithKeyTypeValueType<KeyType, ValueType>(accessType, typeId, hashTable, func);
        }
    } else {
        if (typeId.hasTTL) {
            using ValueType = TimestampValue<offset_t>;
            CollectWithKeyTypeValueType<KeyType, ValueType>(accessType, typeId, hashTable, func);
        } else {
            using ValueType = OffsetValue<offset_t>;
            CollectWithKeyTypeValueType<KeyType, ValueType>(accessType, typeId, hashTable, func);
        }
    }
}

#define CollectRecordWithIterator(iterator, valueUnpacker, valueLen, func)                                             \
    while (iterator->IsValid()) {                                                                                      \
        Record record;                                                                                                 \
        record.key = iterator->Key();                                                                                  \
        record.deleted = iterator->IsDeleted();                                                                        \
        autil::StringView offset;                                                                                      \
        valueUnpacker->Unpack(autil::StringView((char*)(&iterator->Value()), valueLen), record.timestamp, offset);     \
        if (!record.deleted) {                                                                                         \
            record.value = autil::StringView((char*)(offset.data()), /*value len*/ 0);                                 \
        }                                                                                                              \
        func(record);                                                                                                  \
        iterator->MoveToNext();                                                                                        \
    }

#define CollectWithHashTable(HashTableType, ValueType)                                                                 \
    using HashIterator = typename HashTableType::Iterator;                                                             \
    auto typeHashTable = dynamic_cast<HashTableType*>(hashTable.get());                                                \
    assert(typeHashTable);                                                                                             \
    std::unique_ptr<HashIterator> iterator(typeHashTable->CreateIterator());                                           \
    assert(iterator);                                                                                                  \
    std::unique_ptr<ValueUnpacker> valueUnpacker(ValueType::CreateValueUnpacker());                                    \
    auto valueLen = sizeof(ValueType);                                                                                 \
    CollectRecordWithIterator(iterator, valueUnpacker, valueLen, func);

template <typename KeyType, typename ValueType>
void VarLenHashTableCollector::CollectWithKeyTypeValueType(HashTableAccessType accessType, const KVTypeId& typeId,
                                                           const std::shared_ptr<HashTableBase>& hashTable,
                                                           const CollectFuncType& func)
{
    switch (accessType) {
    case DENSE_TABLE: {
        using HashTableType = DenseHashTableBase<KeyType, ValueType, false>;
        CollectWithHashTable(HashTableType, ValueType);
        break;
    }
    case CUCKOO_TABLE: {
        using HashTableType = CuckooHashTable<KeyType, ValueType, false>;
        CollectWithHashTable(HashTableType, ValueType);
        break;
    }
    default:
        AUTIL_LOG(ERROR, "access type [%d] is not support, can't collect records", accessType);
        break;
    }
}

} // namespace indexlibv2::index
