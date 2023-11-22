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

#include <memory>

#include "indexlib/codegen/codegen_object.h"
#include "indexlib/common/hash_table/cuckoo_hash_table.h"
#include "indexlib/common/hash_table/cuckoo_hash_table_file_reader.h"
#include "indexlib/common/hash_table/dense_hash_table.h"
#include "indexlib/common/hash_table/dense_hash_table_file_reader.h"
#include "indexlib/common/hash_table/hash_table_base.h"
#include "indexlib/common/hash_table/hash_table_file_reader_base.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_format_options.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class KVSegmentOffsetReader : public codegen::CodegenObject
{
public:
    KVSegmentOffsetReader()
    {
        mIsMultiRegion = false;
        mIsShortOffset = false;
        mHasTs = false;
        mHasHashTableFileReader = false;
        mCodegenShortOffset = false;
        mCodegenHasHashTableFileReader = false;
    }
    ~KVSegmentOffsetReader() {}

public:
    // new version open
    bool Open(const file_system::DirectoryPtr& kvDir, const config::KVIndexConfigPtr& kvIndexConfig);
    // legacy : to delete
    bool Open(const file_system::DirectoryPtr& kvDir, const config::KVIndexConfigPtr& kvIndexConfig,
              bool isOnlineSegment, bool isBuildingSegment);

    inline FL_LAZY(bool) Find(keytype_t key, bool& isDeleted, offset_t& offset, uint64_t& ts,
                              KVMetricsCollector* collector, autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;
    // legacy : to delete
    inline FL_LAZY(bool) Find(keytype_t key, bool& isDeleted, offset_t& offset, regionid_t& regionId, uint64_t& ts,
                              KVMetricsCollector* collector, autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;
    void TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id) override;

private:
    enum TableType { DENSE_TABLE, CUCKOO_TABLE, DENSE_READER, CUCKOO_READER };

private:
    typedef common::HashTableBase HashTable;
    typedef common::HashTableFileReaderBase HashTableFileReader;
    typedef std::shared_ptr<HashTable> HashTablePtr;
    typedef std::shared_ptr<HashTableFileReader> HashTableFileReaderPtr;
    typedef common::ValueUnpacker ValueUnpacker;

    typedef std::shared_ptr<ValueUnpacker> ValueUnpackerPtr;

    typedef common::HashTableBase ShortOffsetHashTable;
    typedef common::HashTableFileReaderBase ShortOffsetHashTableFileReader;
    typedef std::shared_ptr<ShortOffsetHashTable> ShortOffsetHashTablePtr;
    typedef std::shared_ptr<ShortOffsetHashTableFileReader> ShortOffsetHashTableFileReaderPtr;
    typedef common::ValueUnpacker ShortOffsetValueUnpacker;
    typedef std::shared_ptr<ShortOffsetValueUnpacker> ShortOffsetValueUnpackerPtr;

    typedef std::map<std::string, std::string> KVMap;

private:
    std::string GetTableName(KVMap& nameInfo, bool compactBucket, bool isReader);
    bool IsShortOffset(const config::KVIndexConfigPtr& kvIndexConfig, const file_system::DirectoryPtr& kvDir,
                       bool isOnlineSegment, bool isBuildingSegment);
    void* CreateHashTableFileReader(const config::KVIndexConfigPtr& kvIndexConfig, bool isOnlineSegment,
                                    bool isShortOffset);
    void* CreateHashTable(const config::KVIndexConfigPtr& kvIndexConfig, bool isOnlineSegment, bool isShortOffset);
    bool doCollectAllCode() override;

private:
    // TODO move to offset creator
    void* InnerCreate(TableType tableType, bool isOnline, bool isShortOffset,
                      const config::KVIndexConfigPtr& kvIndexConfig, KVMap& nameInfo);

    template <typename KeyType>
    void* InnerCreateWithKeyType(TableType tableType, bool isShortOffset, bool isOnline, bool hasTTL, KVMap& nameInfo);

    template <typename KeyType, typename OffsetType>
    void* InnerCreateWithOffsetType(TableType tableType, bool isOnline, bool hasTTL, KVMap& nameInfo);
    template <typename KeyType, typename ValueType, bool hasSpecialKey>
    void* InnerCreateTable(TableType tableType, KVMap& nameInfo);
    void CollectCodegenTableName(KVMap& nameInfo);

private:
    HashTableFileReaderPtr mHashTableFileReader;
    HashTablePtr mHashTable;
    ValueUnpackerPtr mValueUnpacker;

    ShortOffsetHashTableFileReaderPtr mShortOffsetHashTableFileReader;
    ShortOffsetHashTablePtr mShortOffsetHashTable;
    ShortOffsetValueUnpackerPtr mShortOffsetValueUnpacker;

    file_system::FileReaderPtr mKeyFileReader;

    // for codegen
    bool mIsMultiRegion;
    bool mIsShortOffset;
    bool mHasTs;
    bool mHasHashTableFileReader;

    // guide codegen
    std::string mHashTableName;           // for codegen
    std::string mHashTableFileReaderName; // for codegen
    std::string mUnpackerName;
    std::string mShortOffsetHashTableName;           // for codegen
    std::string mShortOffsetHashTableFileReaderName; // for codegen
    std::string mShortOffsetUnpackerName;
    bool mCodegenShortOffset;
    bool mCodegenHasHashTableFileReader;

private:
    IE_LOG_DECLARE();
};
///////////////////////////////////////////////////////////////
inline bool KVSegmentOffsetReader::IsShortOffset(const config::KVIndexConfigPtr& kvIndexConfig,
                                                 const file_system::DirectoryPtr& kvDir, bool isOnlineSegment,
                                                 bool isBuildingSegment)
{
    bool isMultiRegion = kvIndexConfig->GetRegionCount() > 1;
    if (isMultiRegion) {
        mCodegenShortOffset = true;
        return false;
    }
    config::KVIndexPreference preference = kvIndexConfig->GetIndexPreference();
    if (!preference.GetHashDictParam().HasEnableShortenOffset()) {
        mCodegenShortOffset = true;
        return false;
    }
    if (isBuildingSegment || isOnlineSegment) {
        mCodegenShortOffset = true;
        return true;
    }
    if (!kvDir->IsExist(KV_FORMAT_OPTION_FILE_NAME)) {
        // legacy code: for rt dumped segment or offline segment
        return false;
    }
    std::string content;
    kvDir->Load(KV_FORMAT_OPTION_FILE_NAME, content);
    KVFormatOptions options;
    options.FromString(content);
    return options.IsShortOffset();
}

template <typename KeyType>
inline void* KVSegmentOffsetReader::InnerCreateWithKeyType(TableType tableType, bool isShortOffset, bool isOnline,
                                                           bool hasTTL, KVMap& nameInfo)
{
    if (isShortOffset) {
        nameInfo["OffsetType"] = "short_offset_t";
        return InnerCreateWithOffsetType<KeyType, short_offset_t>(tableType, isOnline, hasTTL, nameInfo);
    }
    nameInfo["OffsetType"] = "offset_t";
    return InnerCreateWithOffsetType<KeyType, offset_t>(tableType, isOnline, hasTTL, nameInfo);
}

template <typename KeyType, typename ValueType, bool hasSpecialKey>
inline void* KVSegmentOffsetReader::InnerCreateTable(TableType tableType, KVMap& nameInfo)
{
    if (!mIsShortOffset) {
        mValueUnpacker.reset((ValueUnpacker*)ValueType::CreateValueUnpacker());
    } else {
        mShortOffsetValueUnpacker.reset((ShortOffsetValueUnpacker*)ValueType::CreateValueUnpacker());
    }
    switch (tableType) {
    case DENSE_TABLE:
        nameInfo["TableType"] = "common::DenseHashTable";
        nameInfo["TableReaderType"] = "common::DenseHashTableFileReader";
        return new common::DenseHashTable<KeyType, ValueType, hasSpecialKey>();
    case DENSE_READER:
        nameInfo["TableReaderType"] = "common::DenseHashTableFileReader";
        nameInfo["TableType"] = "common::DenseHashTable";
        return new common::DenseHashTableFileReader<KeyType, ValueType, hasSpecialKey>();
    case CUCKOO_TABLE:
        nameInfo["TableType"] = "common::CuckooHashTable";
        nameInfo["TableReaderType"] = "common::CuckooHashTableFileReader";
        return new common::CuckooHashTable<KeyType, ValueType, hasSpecialKey>();
    case CUCKOO_READER:
        nameInfo["TableType"] = "common::CuckooHashTable";
        nameInfo["TableReaderType"] = "common::CuckooHashTableFileReader";
        return new common::CuckooHashTableFileReader<KeyType, ValueType, hasSpecialKey>();
    }
    return NULL;
}

// template<typename TableType, typename KeyType, typename OffsetType>
template <typename KeyType, typename OffsetType>
inline void* KVSegmentOffsetReader::InnerCreateWithOffsetType(TableType tableType, bool isOnline, bool hasTTL,
                                                              KVMap& nameInfo)
{
    if (isOnline || hasTTL) {
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

inline FL_LAZY(bool) KVSegmentOffsetReader::Find(keytype_t key, bool& isDeleted, offset_t& offset, uint64_t& ts,
                                                 KVMetricsCollector* collector, autil::mem_pool::Pool* pool) const
{
    regionid_t regionId = INVALID_REGIONID;
    FL_CORETURN FL_COAWAIT Find(key, isDeleted, offset, regionId, ts, collector, pool);
}

inline FL_LAZY(bool) KVSegmentOffsetReader::Find(keytype_t key, bool& isDeleted, offset_t& offset, regionid_t& regionId,
                                                 uint64_t& ts, KVMetricsCollector* collector,
                                                 autil::mem_pool::Pool* pool) const
{
    autil::StringView tmpValue;
    util::Status status;
    if (!mHasHashTableFileReader) {
        if (mIsShortOffset) {
            status = mShortOffsetHashTable->Find(key, tmpValue);
        } else {
            status = mHashTable->Find(key, tmpValue);
        }
    } else {
        util::BlockAccessCounter* blockCounter = collector ? collector->GetBlockCounter() : nullptr;
        if (mIsShortOffset) {
            status = FL_COAWAIT mShortOffsetHashTableFileReader->Find(key, tmpValue, blockCounter, pool, nullptr);
        } else {
            status = FL_COAWAIT mHashTableFileReader->Find(key, tmpValue, blockCounter, pool, nullptr);
        }
    }
    if (status == util::NOT_FOUND) {
        FL_CORETURN false;
    }
    autil::StringView realValue;
    if (mIsShortOffset) {
        mShortOffsetValueUnpacker->Unpack(tmpValue, ts, realValue);
    } else {
        mValueUnpacker->Unpack(tmpValue, ts, realValue);
    }

    isDeleted = false;
    if (mIsMultiRegion) {
        uint64_t tmpOffset = *(uint64_t*)realValue.data();
        MultiRegionOffsetFormatter formatter(tmpOffset);
        regionId = formatter.GetRegionId();
        if (status == util::DELETED) {
            isDeleted = true;
            FL_CORETURN true;
        }
        offset = formatter.GetOffset();
        FL_CORETURN true;
    }

    regionId = DEFAULT_REGIONID;
    if (status == util::DELETED) {
        isDeleted = true;
        FL_CORETURN true;
    }
    if (mIsShortOffset) {
        offset = *(uint32_t*)realValue.data();
    } else {
        offset = *(uint64_t*)realValue.data();
    }
    FL_CORETURN true;
}
}} // namespace indexlib::index
