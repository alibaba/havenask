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
#include "indexlib/index/kv/kv_segment_offset_reader.h"

#include "indexlib/index/kv/hash_table_var_creator.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KVSegmentOffsetReader);

bool KVSegmentOffsetReader::Open(const file_system::DirectoryPtr& kvDir, const config::KVIndexConfigPtr& kvIndexConfig)
{
    // segment is built segment
    return Open(kvDir, kvIndexConfig, false, false);
}

bool KVSegmentOffsetReader::Open(const file_system::DirectoryPtr& kvDir, const config::KVIndexConfigPtr& kvIndexConfig,
                                 bool isOnlineSegment, bool isBuildingSegment)
{
    bool isShortOffset = IsShortOffset(kvIndexConfig, kvDir, isOnlineSegment, isBuildingSegment);
    mIsShortOffset = isShortOffset;
    bool isMultiRegion = kvIndexConfig->GetRegionCount() > 1;
    mIsMultiRegion = isMultiRegion;
    if (isOnlineSegment || kvIndexConfig->TTLEnabled()) {
        mHasTs = true;
    }
    IE_LOG(INFO, "open kv segment reader with [%s] offset format in path [%s]", (isShortOffset ? "short" : "long"),
           kvDir->DebugString().c_str());

    if (kvDir->IsExist(KV_KEY_FILE_NAME)) {
        mKeyFileReader = kvDir->CreateFileReader(KV_KEY_FILE_NAME, file_system::FSOT_LOAD_CONFIG);
    } else {
        IE_LOG(ERROR, "file [%s] is not exist in path[%s]", KV_KEY_FILE_NAME, kvDir->DebugString().c_str());
        return false;
    }

    if (!mKeyFileReader) {
        IE_LOG(ERROR, "create key file reader failed in dir[%s]", kvDir->DebugString().c_str());
        return false;
    }
    void* baseAddress = mKeyFileReader->GetBaseAddress();
    if (!isOnlineSegment && !isBuildingSegment) {
        mCodegenHasHashTableFileReader = true;
    }
    bool useFileReader = (baseAddress == nullptr);
    KVMap nameInfo;
    auto hashTableInfo = HashTableVarCreator::CreateHashTableForReader(kvIndexConfig, useFileReader, isOnlineSegment,
                                                                       isShortOffset, nameInfo);
    CollectCodegenTableName(nameInfo);
    if (useFileReader) {
        assert(!mHashTableFileReader);
        mHasHashTableFileReader = true;

        if (isShortOffset) {
            mShortOffsetHashTableFileReader.reset(
                static_cast<ShortOffsetHashTableFileReader*>(hashTableInfo.hashTable.release()));
            mShortOffsetValueUnpacker.reset(
                static_cast<ShortOffsetValueUnpacker*>(hashTableInfo.valueUnpacker.release()));
            return mShortOffsetHashTableFileReader->Init(kvDir, mKeyFileReader);
        }
        mHashTableFileReader.reset(static_cast<HashTableFileReader*>(hashTableInfo.hashTable.release()));
        mValueUnpacker.reset(static_cast<ValueUnpacker*>(hashTableInfo.valueUnpacker.release()));
        return mHashTableFileReader->Init(kvDir, mKeyFileReader);
    }

    if (isShortOffset) {
        mShortOffsetHashTable.reset(static_cast<ShortOffsetHashTable*>(hashTableInfo.hashTable.release()));
        mShortOffsetValueUnpacker.reset(static_cast<ShortOffsetValueUnpacker*>(hashTableInfo.valueUnpacker.release()));
        if (!mShortOffsetHashTable->MountForRead(baseAddress, mKeyFileReader->GetLength())) {
            IE_LOG(ERROR, "mount hash table failed, file[%s], length[%lu]", mKeyFileReader->DebugString().c_str(),
                   mKeyFileReader->GetLength());
            return false;
        }
        return true;
    }
    mHashTable.reset(static_cast<HashTable*>(hashTableInfo.hashTable.release()));
    mValueUnpacker.reset(static_cast<ValueUnpacker*>(hashTableInfo.valueUnpacker.release()));
    if (!mHashTable->MountForRead(baseAddress, mKeyFileReader->GetLength())) {
        IE_LOG(ERROR, "mount hash table failed, file[%s], length[%lu]", mKeyFileReader->DebugString().c_str(),
               mKeyFileReader->GetLength());
        return false;
    }
    return true;
}

void KVSegmentOffsetReader::CollectCodegenTableName(KVMap& nameInfo)
{
    mHashTableName = GetTableName(nameInfo, false, false);
    mHashTableFileReaderName = GetTableName(nameInfo, false, true);
    mShortOffsetHashTableName = GetTableName(nameInfo, true, false);
    mShortOffsetHashTableFileReaderName = GetTableName(nameInfo, true, true);
    mUnpackerName = nameInfo["ValueType"] + "::ValueUnpackerType";
    mShortOffsetUnpackerName = nameInfo["ShortOffsetValueType"] + "::ValueUnpackerType";
}

string KVSegmentOffsetReader::GetTableName(KVMap& nameInfo, bool shortOffset, bool isReader)
{
    string valueType = nameInfo["ValueType"];
    if (shortOffset) {
        valueType = nameInfo["ShortOffsetValueType"];
    }
    string tableType = nameInfo["TableType"];
    if (isReader) {
        tableType = nameInfo["TableReaderType"];
    }
    stringstream ss;
    ss << tableType << "<" << nameInfo["KeyType"] << "," << valueType << "," << nameInfo["HasSpecialKey"] << ","
       << "false>";
    string tmpStr = ss.str();
    return tmpStr;
}

void* KVSegmentOffsetReader::InnerCreate(TableType tableType, bool isOnline, bool isShortOffset,
                                         const config::KVIndexConfigPtr& kvIndexConfig, KVMap& nameInfo)
{
    bool compactHashKey = kvIndexConfig->IsCompactHashKey();
    bool isMultiRegion = kvIndexConfig->GetRegionCount() > 1;
    bool hasTTL = kvIndexConfig->TTLEnabled();
    if (isMultiRegion) {
        if (isOnline || hasTTL) {
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
        return InnerCreateWithKeyType<compact_keytype_t>(tableType, isShortOffset, isOnline, hasTTL, nameInfo);
    }
    nameInfo["KeyType"] = "keytype_t";
    return InnerCreateWithKeyType<keytype_t>(tableType, isShortOffset, isOnline, hasTTL, nameInfo);
}

bool KVSegmentOffsetReader::doCollectAllCode()
{
    INIT_CODEGEN_INFO(KVSegmentOffsetReader, true);
    COLLECT_CONST_MEM_VAR(mIsMultiRegion);
    if (mCodegenShortOffset) {
        COLLECT_CONST_MEM_VAR(mIsShortOffset);
    }
    COLLECT_CONST_MEM_VAR(mHasTs);
    if (mCodegenHasHashTableFileReader) {
        COLLECT_CONST_MEM_VAR(mHasHashTableFileReader);
    }
    COLLECT_TYPE_REDEFINE(HashTableFileReader, mHashTableFileReaderName);
    COLLECT_TYPE_REDEFINE(ShortOffsetHashTableFileReader, mShortOffsetHashTableFileReaderName);
    COLLECT_TYPE_REDEFINE(HashTable, mHashTableName);
    COLLECT_TYPE_REDEFINE(ShortOffsetHashTable, mShortOffsetHashTableName);
    COLLECT_TYPE_REDEFINE(ValueUnpacker, mUnpackerName);
    COLLECT_TYPE_REDEFINE(ShortOffsetValueUnpacker, mShortOffsetUnpackerName);
    return true;
}

void KVSegmentOffsetReader::TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id)
{
    codegen::CodegenCheckerPtr checker(new codegen::CodegenChecker);
    COLLECT_CONST_MEM(checker, mIsMultiRegion);
    COLLECT_CONST_MEM(checker, mIsShortOffset);
    COLLECT_CONST_MEM(checker, mHasTs);
    COLLECT_CONST_MEM(checker, mHasHashTableFileReader);
    COLLECT_TYPE_DEFINE(checker, HashTableFileReader);
    COLLECT_TYPE_DEFINE(checker, ShortOffsetHashTableFileReader);
    COLLECT_TYPE_DEFINE(checker, HashTable);
    COLLECT_TYPE_DEFINE(checker, ShortOffsetHashTable);
    COLLECT_TYPE_DEFINE(checker, ValueUnpacker);
    COLLECT_TYPE_DEFINE(checker, ShortOffsetValueUnpacker);
    checkers[string("KVSegmentOffsetReader") + id] = checker;
}
}} // namespace indexlib::index
