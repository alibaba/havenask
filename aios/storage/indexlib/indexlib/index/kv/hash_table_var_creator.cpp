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
#include "indexlib/index/kv/hash_table_var_creator.h"

#include "indexlib/common/hash_table/bucket_offset_compressor.h"
#include "indexlib/common/hash_table/closed_hash_table_file_iterator.h"
#include "indexlib/common/hash_table/cuckoo_hash_table.h"
#include "indexlib/common/hash_table/cuckoo_hash_table_file_reader.h"
#include "indexlib/common/hash_table/dense_hash_table.h"
#include "indexlib/common/hash_table/dense_hash_table_file_reader.h"
#include "indexlib/common/hash_table/hash_table_file_reader_base.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_factory.h"
#include "indexlib/index/kv/kv_typeid.h"

using namespace std;

using namespace indexlib::common;

namespace indexlib { namespace index {

IE_LOG_SETUP(index, HashTableVarCreator);

HashTableVarCreator::HashTableVarCreator() {}

HashTableVarCreator::~HashTableVarCreator() {}

HashTableInfo HashTableVarCreator::CreateHashTableForReader(const config::KVIndexConfigPtr& kvIndexConfig,
                                                            bool useFileReader, bool isRtSegment, bool isShortOffset,
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

    return InnerCreate(tt, kvIndexConfig, isRtSegment, isShortOffset, nameInfo);
}

HashTableInfo HashTableVarCreator::CreateHashTableForWriter(const config::KVIndexConfigPtr& kvIndexConfig,
                                                            const KVFormatOptionsPtr& kvOptions, bool isRtSegment)
{
    bool isShortOffset = KVFactory::GetKVTypeId(kvIndexConfig, kvOptions).shortOffset;
    config::KVIndexPreference preference = kvIndexConfig->GetIndexPreference();
    std::string hashTypeStr = preference.GetHashDictParam().GetHashType();
    HashTableAccessType tt = DENSE_TABLE;
    bool useFileReader = false;
    KVMap nameInfo;
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
    return InnerCreate(tt, kvIndexConfig, isRtSegment, isShortOffset, nameInfo);
}

KVVarOffsetFormatterBase* HashTableVarCreator::CreateKVVarOffsetFormatter(const config::KVIndexConfigPtr& kvIndexConfig,
                                                                          bool isRtSegment)
{
    bool isShortOffset = KVFactory::GetKVTypeId(kvIndexConfig, KVFormatOptionsPtr()).shortOffset;
    config::KVIndexPreference preference = kvIndexConfig->GetIndexPreference();
    std::string hashTypeStr = preference.GetHashDictParam().GetHashType();
    bool isMultiRegion = kvIndexConfig->GetRegionCount() > 1;
    bool hasTTL = (kvIndexConfig->TTLEnabled() || isRtSegment);
    if (isMultiRegion) {
        if (hasTTL) {
            return new KVVarOffsetFormatter<MultiRegionTimestampValue<offset_t>>();
        } else {
            return new KVVarOffsetFormatter<MultiRegionTimestamp0Value<offset_t>>();
        }
    }
    if (isShortOffset) {
        if (hasTTL) {
            return new KVVarOffsetFormatter<TimestampValue<short_offset_t>>();
        } else {
            return new KVVarOffsetFormatter<OffsetValue<short_offset_t, std::numeric_limits<short_offset_t>::max(),
                                                        std::numeric_limits<short_offset_t>::max() - 1>>();
        }
    } else {
        if (hasTTL) {
            return new KVVarOffsetFormatter<TimestampValue<offset_t>>();
        } else {
            return new KVVarOffsetFormatter<OffsetValue<offset_t, std::numeric_limits<offset_t>::max(),
                                                        std::numeric_limits<offset_t>::max() - 1>>();
        }
    }
}

HashTableInfo HashTableVarCreator::InnerCreate(HashTableAccessType tableType,
                                               const config::KVIndexConfigPtr& kvIndexConfig, bool isRtSegment,
                                               bool isShortOffset, KVMap& nameInfo)
{
    bool compactHashKey = kvIndexConfig->IsCompactHashKey();
    bool isMultiRegion = kvIndexConfig->GetRegionCount() > 1;
    bool hasTTL = (kvIndexConfig->TTLEnabled() || isRtSegment);
    if (isMultiRegion) {
        if (hasTTL) {
            typedef MultiRegionTimestampValue<offset_t> ValueType;
            nameInfo["KeyType"] = "keytype_t";
            nameInfo["ValueType"] = "MultiRegionTimestampValue<offset_t>";
            nameInfo["ShortOffsetValueType"] = "common::TimestampValue<short_offset_t>";
            nameInfo["HasSpecialKey"] = "false";
            return InnerCreateTable<keytype_t, ValueType, false>(tableType, nameInfo);
        } else {
            typedef MultiRegionTimestamp0Value<offset_t> ValueType;
            nameInfo["KeyType"] = "keytype_t";
            nameInfo["ValueType"] = "MultiRegionTimestamp0Value<offset_t>";
            nameInfo["ShortOffsetValueType"] = "common::TimestampValue<short_offset_t>";
            nameInfo["HasSpecialKey"] = "false";
            return InnerCreateTable<keytype_t, ValueType, false>(tableType, nameInfo);
        }
    }

    if (compactHashKey) {
        nameInfo["KeyType"] = "compact_keytype_t";
        return InnerCreateWithKeyType<compact_keytype_t>(tableType, isShortOffset, hasTTL, nameInfo);
    }
    nameInfo["KeyType"] = "keytype_t";
    return InnerCreateWithKeyType<keytype_t>(tableType, isShortOffset, hasTTL, nameInfo);
}

template <typename KeyType>
HashTableInfo HashTableVarCreator::InnerCreateWithKeyType(HashTableAccessType tableType, bool isShortOffset,
                                                          bool hasTTL, KVMap& nameInfo)
{
    if (isShortOffset) {
        nameInfo["OffsetType"] = "short_offset_t";
        return InnerCreateWithOffsetType<KeyType, short_offset_t>(tableType, hasTTL, nameInfo);
    }
    nameInfo["OffsetType"] = "offset_t";
    return InnerCreateWithOffsetType<KeyType, offset_t>(tableType, hasTTL, nameInfo);
}

template <typename KeyType, typename OffsetType>
HashTableInfo HashTableVarCreator::InnerCreateWithOffsetType(HashTableAccessType tableType, bool hasTTL,
                                                             KVMap& nameInfo)
{
    if (hasTTL) {
        typedef common::TimestampValue<OffsetType> ValueType;
        nameInfo["ValueType"] = "common::TimestampValue<offset_t>";
        nameInfo["ShortOffsetValueType"] = "common::TimestampValue<short_offset_t>";
        nameInfo["HasSpecialKey"] = "false";
        return InnerCreateTable<KeyType, ValueType, false>(tableType, nameInfo);
    }
    typedef common::OffsetValue<OffsetType, std::numeric_limits<OffsetType>::max(),
                                std::numeric_limits<OffsetType>::max() - 1>
        ValueType;
    nameInfo["ValueType"] = "common::OffsetValue<offset_t"
                            ", std::numeric_limits<offset_t"
                            ">::max(), std::numeric_limits<offset_t"
                            ">::max() - 1>";
    nameInfo["ShortOffsetValueType"] = "common::OffsetValue<short_offset_t, "
                                       "std::numeric_limits<short_offset_t>::max(),"
                                       " std::numeric_limits<short_offset_t>::max() - 1>";
    nameInfo["HasSpecialKey"] = "false";
    return InnerCreateTable<KeyType, ValueType, false>(tableType, nameInfo);
}

template <typename KeyType, typename ValueType, bool hasSpecialKey>
HashTableInfo HashTableVarCreator::InnerCreateTable(HashTableAccessType tableType, KVMap& nameInfo)
{
    HashTableInfo retInfo;
    switch (tableType) {
    case DENSE_TABLE: {
        nameInfo["TableType"] = "common::DenseHashTable";
        nameInfo["TableReaderType"] = "common::DenseHashTableFileReader";
        using HashTableInst = common::DenseHashTable<KeyType, ValueType, hasSpecialKey>;
        retInfo.hashTable.reset(new HashTableInst());
        retInfo.bucketCompressor.reset(new BucketOffsetCompressor<typename HashTableInst::Bucket>());
        retInfo.hashTableFileIterator.reset(
            new ClosedHashTableFileIterator<typename HashTableInst::HashTableHeader, KeyType, ValueType, hasSpecialKey,
                                            true, false>());
        break;
    }
    case DENSE_READER: {
        nameInfo["TableType"] = "common::DenseHashTable";
        nameInfo["TableReaderType"] = "common::DenseHashTableFileReader";
        using HashTableInst = common::DenseHashTableFileReader<KeyType, ValueType, hasSpecialKey>;
        retInfo.hashTable.reset(new HashTableInst());
        retInfo.hashTableFileIterator.reset(
            new ClosedHashTableFileIterator<typename HashTableInst::HashTableHeader, KeyType, ValueType, hasSpecialKey,
                                            true, false>());
        break;
    }
    case CUCKOO_TABLE: {
        nameInfo["TableType"] = "common::CuckooHashTable";
        nameInfo["TableReaderType"] = "common::CuckooHashTableFileReader";
        using HashTableInst = common::CuckooHashTable<KeyType, ValueType, hasSpecialKey>;
        retInfo.hashTable.reset(new HashTableInst());
        retInfo.bucketCompressor.reset(new BucketOffsetCompressor<typename HashTableInst::Bucket>());
        retInfo.hashTableFileIterator.reset(
            new ClosedHashTableFileIterator<typename HashTableInst::HashTableHeader, KeyType, ValueType, hasSpecialKey,
                                            true, false>());
        break;
    }
    case CUCKOO_READER: {
        nameInfo["TableType"] = "common::CuckooHashTable";
        nameInfo["TableReaderType"] = "common::CuckooHashTableFileReader";
        using HashTableInst = common::CuckooHashTableFileReader<KeyType, ValueType, hasSpecialKey>;
        retInfo.hashTable.reset(new HashTableInst());
        retInfo.hashTableFileIterator.reset(
            new ClosedHashTableFileIterator<typename HashTableInst::HashTableHeader, KeyType, ValueType, hasSpecialKey,
                                            true, false>());
        break;
    }
    }

    retInfo.valueUnpacker.reset(ValueType::CreateValueUnpacker());
    assert(retInfo.hashTable.operator bool());
    assert(retInfo.valueUnpacker.operator bool());
    return retInfo;
}
}} // namespace indexlib::index
