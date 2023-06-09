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

#include "indexlib/common/hash_table/closed_hash_table_buffered_file_iterator.h"
#include "indexlib/common/hash_table/cuckoo_hash_table.h"
#include "indexlib/common/hash_table/cuckoo_hash_table_file_reader.h"
#include "indexlib/common/hash_table/dense_hash_table.h"
#include "indexlib/common/hash_table/dense_hash_table_file_reader.h"
// #include "indexlib/index/kv/hash_table_fix_creator.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_typeid.h"

namespace indexlib { namespace index {

template <typename KeyType, typename ValueType, bool hasSpecialKey, bool compactBucket>
common::HashTableInfo InnerCreateTable(common::HashTableAccessType tableType, common::KVMap& nameInfo)
{
    common::HashTableAccessor* table = nullptr;
    common::HashTableFileIterator* iterator = nullptr;
    switch (tableType) {
    case common::DENSE_TABLE: {
        nameInfo["TableType"] = "common::DenseHashTable";
        nameInfo["TableReaderType"] = "common::DenseHashTableFileReader";
        using HashTableInst = common::DenseHashTable<KeyType, ValueType, hasSpecialKey, compactBucket>;
        table = new HashTableInst();
        iterator = new common::ClosedHashTableBufferedFileIterator<typename HashTableInst::HashTableHeader, KeyType,
                                                                   ValueType, hasSpecialKey, compactBucket>();
        break;
    }
    case common::DENSE_READER: {
        nameInfo["TableType"] = "common::DenseHashTable";
        nameInfo["TableReaderType"] = "common::DenseHashTableFileReader";
        using HashTableInst = common::DenseHashTableFileReader<KeyType, ValueType, hasSpecialKey, compactBucket>;
        table = new HashTableInst();
        iterator = new common::ClosedHashTableBufferedFileIterator<typename HashTableInst::HashTableHeader, KeyType,
                                                                   ValueType, hasSpecialKey, compactBucket>();
        break;
    }
    case common::CUCKOO_TABLE: {
        nameInfo["TableType"] = "common::CuckooHashTable";
        nameInfo["TableReaderType"] = "common::CuckooHashTableFileReader";
        using HashTableInst = common::CuckooHashTable<KeyType, ValueType, hasSpecialKey, compactBucket>;
        table = new HashTableInst();
        iterator = new common::ClosedHashTableBufferedFileIterator<typename HashTableInst::HashTableHeader, KeyType,
                                                                   ValueType, hasSpecialKey, compactBucket>();
        break;
    }
    case common::CUCKOO_READER: {
        nameInfo["TableType"] = "common::CuckooHashTable";
        nameInfo["TableReaderType"] = "common::CuckooHashTableFileReader";
        using HashTableInst = common::CuckooHashTableFileReader<KeyType, ValueType, hasSpecialKey, compactBucket>;
        table = new HashTableInst();
        iterator = new common::ClosedHashTableBufferedFileIterator<typename HashTableInst::HashTableHeader, KeyType,
                                                                   ValueType, hasSpecialKey, compactBucket>();
        break;
    }
    }
    common::HashTableInfo retInfo;
    retInfo.hashTable.reset(table);
    retInfo.valueUnpacker.reset(ValueType::CreateValueUnpacker());
    retInfo.hashTableFileIterator.reset(iterator);
    assert(retInfo.hashTable.operator bool());
    assert(retInfo.valueUnpacker.operator bool());
    return retInfo;
}

#define HASHTABLEFIXCREATOR_EXTERN_TEMPLATE_INTERNAL_IMPL(FieldType, hasSpecialKey)                                    \
    template common::HashTableInfo InnerCreateTable<keytype_t, FieldType, hasSpecialKey, true>(                        \
        common::HashTableAccessType tableType, common::KVMap & nameInfo);                                              \
    template common::HashTableInfo InnerCreateTable<keytype_t, FieldType, hasSpecialKey, false>(                       \
        common::HashTableAccessType tableType, common::KVMap & nameInfo);                                              \
    template common::HashTableInfo InnerCreateTable<compact_keytype_t, FieldType, hasSpecialKey, true>(                \
        common::HashTableAccessType tableType, common::KVMap & nameInfo);                                              \
    template common::HashTableInfo InnerCreateTable<compact_keytype_t, FieldType, hasSpecialKey, false>(               \
        common::HashTableAccessType tableType, common::KVMap & nameInfo);

#define HASHTABLEFIXCREATOR_EXTERN_TEMPLATE_INTERNAL(FieldType)                                                        \
    HASHTABLEFIXCREATOR_EXTERN_TEMPLATE_INTERNAL_IMPL(                                                                 \
        common::TimestampValue<config::FieldTypeTraits<FieldType>::AttrItemType>, false)                               \
    HASHTABLEFIXCREATOR_EXTERN_TEMPLATE_INTERNAL_IMPL(                                                                 \
        common::Timestamp0Value<config::FieldTypeTraits<FieldType>::AttrItemType>, true)
}} // namespace indexlib::index
