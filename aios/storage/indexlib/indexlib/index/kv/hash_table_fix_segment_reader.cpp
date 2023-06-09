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
#include "indexlib/index/kv/hash_table_fix_segment_reader.h"

#include "indexlib/index/kv/hash_table_fix_creator.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace indexlib::codegen;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, HashTableFixSegmentReader);

void HashTableFixSegmentReader::Open(const config::KVIndexConfigPtr& kvIndexConfig,
                                     const index_base::SegmentData& segmentData)
{
    if (segmentData.GetSegmentInfo()->HasMultiSharding()) {
        INDEXLIB_FATAL_ERROR(UnSupported, "kv can not open segment [%d] with multi sharding data",
                             segmentData.GetSegmentId());
    }

    mSegmentId = segmentData.GetSegmentId();
    bool hasTTL = kvIndexConfig->TTLEnabled();
    bool isOnlineSegment = index_base::RealtimeSegmentDirectory::IsRtSegmentId(segmentData.GetSegmentId());
    if (isOnlineSegment || hasTTL) {
        mHasTTL = true;
    }
    if (!isOnlineSegment) {
        mCodegenHasHashTableReader = true;
    }
    if (isOnlineSegment || !kvIndexConfig->GetIndexPreference().GetValueParam().IsFixLenAutoInline()) {
        mCodegenCompactBucket = true;
    }
    file_system::DirectoryPtr directory = segmentData.GetDirectory();
    file_system::DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, true);
    file_system::DirectoryPtr kvDir = indexDir->GetDirectory(kvIndexConfig->GetIndexName(), true);
    KVFormatOptionsPtr kvOptions(new KVFormatOptions());
    if (kvDir->IsExist(KV_FORMAT_OPTION_FILE_NAME)) {
        std::string content;
        kvDir->Load(KV_FORMAT_OPTION_FILE_NAME, content);
        kvOptions->FromString(content);
    }
    bool useCompactBucket = kvOptions->UseCompactBucket();
    mCompactBucket = useCompactBucket;
    InnerOpen(useCompactBucket, kvDir, segmentData, kvIndexConfig);
    const config::ValueConfigPtr& valueConfig = kvIndexConfig->GetValueConfig();
    if (valueConfig->GetAttributeCount() == 1) {
        const config::AttributeConfigPtr& attrConfig = valueConfig->GetAttributeConfig(0);
        assert(attrConfig);
        mCompressType = attrConfig->GetCompressType();
        if (mCompressType.HasFp16EncodeCompress()) {
            mValueType = indexlibv2::ct_fp16;
        } else if (mCompressType.HasInt8EncodeCompress()) {
            mValueType = indexlibv2::ct_int8;
        } else {
            mValueType = indexlibv2::ct_other;
        }
    }
    mValueSize = mValueUnpacker->GetValueSize();
}

void HashTableFixSegmentReader::InnerOpen(bool useCompactBucket, const file_system::DirectoryPtr& kvDir,
                                          const index_base::SegmentData& segmentData,
                                          const config::KVIndexConfigPtr& kvIndexConfig)
{
    mFileReader = CreateFileReader(kvDir);
    if (!mFileReader) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "kv open key file fail for segment [%d]", segmentData.GetSegmentId());
    }
    void* baseAddress = mFileReader->GetBaseAddress();
    KVMap nameInfo;
    bool isRtSegment = index_base::RealtimeSegmentDirectory::IsRtSegmentId(segmentData.GetSegmentId());
    bool useFileReader = (baseAddress == nullptr);
    auto hashTableInfo = HashTableFixCreator::CreateHashTableForReader(kvIndexConfig, useFileReader, useCompactBucket,
                                                                       isRtSegment, nameInfo);
    CollectCodegenTableName(nameInfo);

    mValueUnpacker.reset((ValueUnpacker*)(hashTableInfo.valueUnpacker.release()));

    if (useFileReader) {
        if (!useCompactBucket) {
            mHashTableFileReader.reset(static_cast<HashTableFileReader*>(hashTableInfo.hashTable.release()));
            if (!mHashTableFileReader->Init(kvDir, mFileReader)) {
                INDEXLIB_FATAL_ERROR(IndexCollapsed, "init hash table file reader fail for file [%s] in segment [%d]",
                                     mFileReader->DebugString().c_str(), segmentData.GetSegmentId());
            }
        } else {
            mCompactHashTableFileReader.reset(
                static_cast<CompactHashTableFileReader*>(hashTableInfo.hashTable.release()));
            if (!mCompactHashTableFileReader->Init(kvDir, mFileReader)) {
                INDEXLIB_FATAL_ERROR(IndexCollapsed, "init hash table file reader fail for file [%s] in segment [%d]",
                                     mFileReader->DebugString().c_str(), segmentData.GetSegmentId());
            }
        }
        mHasHashTableReader = true;
        return;
    }
    if (!useCompactBucket) {
        mHashTable.reset(static_cast<HashTable*>(hashTableInfo.hashTable.release()));
        if (!mHashTable->MountForRead(baseAddress, mFileReader->GetLength())) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "hash table file [%s] open failed in segment [%d]",
                                 mFileReader->DebugString().c_str(), segmentData.GetSegmentId());
        }
    } else {
        mCompactHashTable.reset(static_cast<CompactHashTable*>(hashTableInfo.hashTable.release()));
        if (!mCompactHashTable->MountForRead(baseAddress, mFileReader->GetLength())) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "hash table file [%s] open failed in segment [%d]",
                                 mFileReader->DebugString().c_str(), segmentData.GetSegmentId());
        }
    }
}

void HashTableFixSegmentReader::CollectCodegenTableName(KVMap& nameInfo)
{
    mHashTableName = GetTableName(nameInfo, false, false);
    mHashTableFileReaderName = GetTableName(nameInfo, false, true);
    mCompactHashTableName = GetTableName(nameInfo, true, false);
    mCompactHashTableFileReaderName = GetTableName(nameInfo, true, true);
    mUnpackerName = nameInfo["ValueType"] + "::ValueUnpackerType";
}

string HashTableFixSegmentReader::GetTableName(KVMap& nameInfo, bool compactBucket, bool isReader)
{
    string compactBucketStr = "false";
    if (compactBucket) {
        compactBucketStr = "true";
    }
    string tableType = nameInfo["TableType"];
    if (isReader) {
        tableType = nameInfo["TableReaderType"];
    }
    stringstream ss;
    ss << tableType << "<" << nameInfo["KeyType"] << "," << nameInfo["ValueType"] << "," << nameInfo["HasSpecialKey"]
       << "," << compactBucketStr << ">";
    string tmpStr = ss.str();
    return tmpStr;
}

file_system::FileReaderPtr HashTableFixSegmentReader::CreateFileReader(const file_system::DirectoryPtr& directory)
{
    if (directory->IsExist(KV_KEY_FILE_NAME)) {
        return directory->CreateFileReader(KV_KEY_FILE_NAME, file_system::FSOT_LOAD_CONFIG);
    }
    return file_system::FileReaderPtr();
}

bool HashTableFixSegmentReader::doCollectAllCode()
{
    INIT_CODEGEN_INFO(HashTableFixSegmentReader, true);
    COLLECT_CONST_MEM_VAR(mHasTTL);
    COLLECT_CONST_MEM_VAR(mValueType);
    if (mCodegenHasHashTableReader) {
        COLLECT_CONST_MEM_VAR(mHasHashTableReader);
    }
    if (mCodegenCompactBucket) {
        COLLECT_CONST_MEM_VAR(mCompactBucket);
    }
    COLLECT_TYPE_REDEFINE(HashTableFileReader, mHashTableFileReaderName);
    COLLECT_TYPE_REDEFINE(HashTable, mHashTableName);
    COLLECT_TYPE_REDEFINE(CompactHashTableFileReader, mCompactHashTableFileReaderName);
    COLLECT_TYPE_REDEFINE(CompactHashTable, mCompactHashTableName);
    COLLECT_TYPE_REDEFINE(ValueUnpacker, mUnpackerName);
    return true;
}

void HashTableFixSegmentReader::TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id)
{
    codegen::CodegenCheckerPtr checker(new CodegenChecker);
    COLLECT_CONST_MEM(checker, mHasTTL);
    COLLECT_CONST_MEM(checker, mValueType);
    COLLECT_CONST_MEM(checker, mHasHashTableReader);
    COLLECT_CONST_MEM(checker, mCompactBucket);
    COLLECT_TYPE_DEFINE(checker, HashTableFileReader);
    COLLECT_TYPE_DEFINE(checker, HashTable);
    COLLECT_TYPE_DEFINE(checker, CompactHashTableFileReader);
    COLLECT_TYPE_DEFINE(checker, CompactHashTable);
    COLLECT_TYPE_DEFINE(checker, ValueUnpacker);
    checkers[string("HashTableFixSegmentReader") + id] = checker;
}
}} // namespace indexlib::index
