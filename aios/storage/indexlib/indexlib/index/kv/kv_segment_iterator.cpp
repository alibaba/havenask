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
#include "indexlib/index/kv/kv_segment_iterator.h"

#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/index/kv/hash_table_fix_creator.h"
#include "indexlib/index/kv/hash_table_var_creator.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_define.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KVSegmentIterator);

bool KVSegmentIterator::Open(const config::IndexPartitionSchemaPtr& schema, const KVFormatOptionsPtr& kvOptions,
                             const index_base::SegmentData& segmentData, bool needRandomAccess)
{
    auto kvIndexConfig = CreateDataKVIndexConfig(schema);
    mIsHashTableVarSegment = IsVarLenHashTable(kvIndexConfig);
    mIsShortOffset = kvOptions->IsShortOffset();

    bool isRtSegment = index_base::RealtimeSegmentDirectory::IsRtSegmentId(segmentData.GetSegmentId());
    assert(!isRtSegment);

    common::HashTableInfo hashTableInfo;
    common::KVMap nameInfo;
    if (mIsHashTableVarSegment) {
        hashTableInfo = HashTableVarCreator::CreateHashTableForReader(kvIndexConfig, false, isRtSegment,
                                                                      kvOptions->IsShortOffset(), nameInfo);
    } else {
        hashTableInfo = HashTableFixCreator::CreateHashTableForReader(
            kvIndexConfig, false, kvOptions->UseCompactBucket(), isRtSegment, nameInfo);
    }
    mUnpacker = std::move(hashTableInfo.valueUnpacker);
    mHashTableIterator = std::move(hashTableInfo.hashTableFileIterator);

    file_system::DirectoryPtr directory = segmentData.GetDirectory();
    file_system::DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, true);
    file_system::DirectoryPtr kvDir = indexDir->GetDirectory(kvIndexConfig->GetIndexName(), true);

    if (!OpenKey(kvDir)) {
        return false;
    }

    if (needRandomAccess) {
        IE_LOG(INFO, "mount file for random access [%s]", kvDir->DebugString().c_str());
        mKeyFileReaderForHash = kvDir->CreateFileReader(KV_KEY_FILE_NAME, file_system::FSOT_MEM);
        mHashTable.reset(static_cast<common::HashTableBase*>(hashTableInfo.hashTable.release()));
        mHashTable->MountForRead(mKeyFileReaderForHash->GetBaseAddress(), mKeyFileReaderForHash->GetLength());
    }

    if (mIsHashTableVarSegment) {
        mHashTableIterator->SortByValue();
        mOffsetFormatter.reset(HashTableVarCreator::CreateKVVarOffsetFormatter(kvIndexConfig, false));

        if (!OpenValue(kvDir, kvIndexConfig)) {
            return false;
        }
        mFixValueLens.reserve(schema->GetRegionCount());
        for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++) {
            config::KVIndexConfigPtr kvConfig =
                DYNAMIC_POINTER_CAST(config::KVIndexConfig, schema->GetIndexSchema(id)->GetPrimaryKeyIndexConfig());
            mFixValueLens.push_back(kvConfig->GetValueConfig()->GetFixedLength());
        }

        mBufferSize = INIT_BUFFER_SIZE;
        mBuffer = new char[mBufferSize];
        mPrevOffset = 0;
        return true;
    }
    return true;
}

bool KVSegmentIterator::OpenKey(const file_system::DirectoryPtr& kvDir)
{
    mKeyFileReader =
        kvDir->CreateFileReader(KV_KEY_FILE_NAME, file_system::ReaderOption::CacheFirst(file_system::FSOT_BUFFERED));
    if (!mKeyFileReader) {
        IE_LOG(ERROR, "open file failed, file[%s], FSOpenType [%d]", mKeyFileReader->DebugString().c_str(),
               mKeyFileReader->GetOpenType());
        return false;
    }

    if (!mHashTableIterator->Init(mKeyFileReader)) {
        IE_LOG(ERROR, "create hash file iterator failed. file[%s]", mKeyFileReader->DebugString().c_str());
        return false;
    }
    return true;
}

bool KVSegmentIterator::OpenValue(const file_system::DirectoryPtr& kvDir, const config::KVIndexConfigPtr& kvIndexConfig)
{
    file_system::CompressFileInfoPtr compressInfo = kvDir->GetCompressFileInfo(KV_VALUE_FILE_NAME);
    if (compressInfo) {
        const config::KVIndexPreference::ValueParam& valueParam = kvIndexConfig->GetIndexPreference().GetValueParam();
        if (valueParam.GetFileCompressType() != compressInfo->compressorName) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "file compressor not equal: schema [%s], index [%s]",
                                 valueParam.GetFileCompressType().c_str(), compressInfo->compressorName.c_str());
        }
    }

    file_system::ReaderOption readerOption = file_system::ReaderOption::CacheFirst(file_system::FSOT_BUFFERED);
    readerOption.supportCompress = (compressInfo != nullptr);
    mValueFileReader = kvDir->CreateFileReader(KV_VALUE_FILE_NAME, readerOption);
    assert(mValueFileReader);
    return true;
}

void KVSegmentIterator::GetKey(keytype_t& key, bool& isDeleted)
{
    key = mHashTableIterator->GetKey();
    isDeleted = mHashTableIterator->IsDeleted();
}

void KVSegmentIterator::GetValue(autil::StringView& value, uint32_t& timestamp, regionid_t& regionId, bool isDeleted)
{
    auto valueStr = mHashTableIterator->GetValue();
    if (mIsHashTableVarSegment) {
        offset_t offset = 0;
        if (mIsShortOffset) {
            short_offset_t shortOffset = 0;
            mUnpacker->UnpackRealType(valueStr, timestamp, shortOffset);
            offset = shortOffset;
        } else {
            mUnpacker->UnpackRealType(valueStr, timestamp, offset);
        }
        regionId = mOffsetFormatter->GetRegionId(offset);
        if (!isDeleted) {
            ReadValue(mOffsetFormatter->GetOffset(offset), regionId, value);
        }
    } else {
        mUnpacker->Unpack(valueStr, timestamp, value);
        regionId = DEFAULT_REGIONID;
    }
}

void KVSegmentIterator::Get(keytype_t& key, autil::StringView& value, uint32_t& timestamp, bool& isDeleted,
                            regionid_t& regionId)
{
    GetKey(key, isDeleted);
    GetValue(value, timestamp, regionId, isDeleted);
}

util::Status KVSegmentIterator::Find(uint64_t key, autil::StringView& value) const
{
    assert(mHashTable);
    return mHashTable->Find(key, value);
}

bool KVSegmentIterator::Seek(int64_t offset) { return mHashTableIterator->Seek(offset); }

int64_t KVSegmentIterator::GetOffset() const { return mHashTableIterator->GetOffset(); }

size_t KVSegmentIterator::EstimateMemoryUse(const config::KVIndexConfigPtr& kvIndexConfig,
                                            const KVFormatOptionsPtr& kvOptions,
                                            const framework::SegmentMetricsVec& segmentMetrics,
                                            const index_base::SegmentTopologyInfo& targetTopoInfo)
{
    if (IsVarLenHashTable(kvIndexConfig)) {
        uint64_t maxKeyCount = 0;
        const std::string& groupName = index_base::SegmentMetricsUtil::GetColumnGroupName(targetTopoInfo.columnIdx);
        for (size_t i = 0; i < segmentMetrics.size(); ++i) {
            uint64_t keyCount = segmentMetrics[i]->Get<size_t>(groupName, KV_KEY_COUNT);
            if (maxKeyCount < keyCount) {
                maxKeyCount = keyCount;
            }
        }
        common::KVMap nameInfo;
        auto hashTableInfo = HashTableVarCreator::CreateHashTableForReader(kvIndexConfig, false, false,
                                                                           kvOptions->IsShortOffset(), nameInfo);

        return hashTableInfo.hashTableFileIterator->EstimateMemoryUse(maxKeyCount) +
               file_system::ReaderOption::DEFAULT_BUFFER_SIZE * 2;
    }
    return file_system::ReaderOption::DEFAULT_BUFFER_SIZE;
}

void KVSegmentIterator::ReadValue(offset_t offset, regionid_t regionId, autil::StringView& value)
{
    if (offset < mPrevOffset) {
        IE_LOG(ERROR, "unordered offset[%ld], prevOffset[%ld]", offset, mPrevOffset);
        assert(false);
    }
    mPrevOffset = offset;

    size_t encodeCountLen = 0;
    size_t itemLen = 0;

    if (regionId >= (regionid_t)mFixValueLens.size()) {
        IE_LOG(ERROR, "regionId [%d] out of schema region count [%lu]", regionId, mFixValueLens.size());
        return;
    }

    if (mFixValueLens[regionId] > 0) {
        itemLen = mFixValueLens[regionId];
    } else {
        // read encodeCount
        size_t retLen = mValueFileReader->Read(mBuffer, sizeof(uint8_t), offset).GetOrThrow();
        if (retLen != sizeof(uint8_t)) {
            IE_LOG(ERROR, "read encodeCount first bytes failed from file[%s]", mValueFileReader->DebugString().c_str());
            return;
        }
        encodeCountLen = common::VarNumAttributeFormatter::GetEncodedCountFromFirstByte(*mBuffer);
        assert(encodeCountLen < mBufferSize);
        retLen = mValueFileReader->Read(mBuffer + sizeof(uint8_t), encodeCountLen - 1, offset + sizeof(uint8_t))
                     .GetOrThrow();
        if (retLen != encodeCountLen - 1) {
            IE_LOG(ERROR, "read encodeCount from file[%s]", mValueFileReader->DebugString().c_str());
            return;
        }
        bool isNull = false;
        uint32_t itemCount = common::VarNumAttributeFormatter::DecodeCount(mBuffer, encodeCountLen, isNull);
        (void)isNull;
        // read value
        itemLen = itemCount * sizeof(char);
    }

    size_t dataLen = itemLen + encodeCountLen;
    if (mBufferSize < dataLen) {
        char* newBuffer = new char[dataLen];
        memcpy(newBuffer, mBuffer, encodeCountLen);
        ARRAY_DELETE_AND_SET_NULL(mBuffer);
        mBuffer = newBuffer;
        mBufferSize = dataLen;
    }

    if (itemLen != mValueFileReader->Read(mBuffer + encodeCountLen, itemLen, offset + encodeCountLen).GetOrThrow()) {
        IE_LOG(ERROR, "read value from file[%s] failed", mValueFileReader->DebugString().c_str());
        return;
    }
    value = {mBuffer, dataLen}; // pack value not actual value
}
}} // namespace indexlib::index
