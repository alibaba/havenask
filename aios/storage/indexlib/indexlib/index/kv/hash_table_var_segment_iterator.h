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

#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/kv_segment_iterator.h"
#include "indexlib/index_base/index_meta/segment_metrics_util.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/ValueWriter.h"

namespace indexlib { namespace index {

template <typename Traits>
class HashTableVarSegmentIterator final : public KVSegmentIterator
{
public:
    HashTableVarSegmentIterator() : mBufferSize(INIT_BUFFER_SIZE), mBuffer(new char[mBufferSize]), mPrevOffset(0) {}
    ~HashTableVarSegmentIterator() { ARRAY_DELETE_AND_SET_NULL(mBuffer); }

public:
    bool Open(const config::IndexPartitionSchemaPtr& schema, const index_base::SegmentData& segmentData) override final;

    bool IsValid() const override final { return mIterator.IsValid(); }

    void MoveToNext() override final { mIterator.MoveToNext(); }

    void Get(keytype_t& key, autil::StringView& value, uint32_t& timestamp, bool& isDeleted,
             regionid_t& regionId) override final
    {
        key = mIterator.Key();
        const T& typedValue = mIterator.Value();
        timestamp = typedValue.Timestamp();
        isDeleted = mIterator.IsDeleted();
        KVVarOffsetFormatter<T> offsetFormatter(typedValue.Value());
        regionId = offsetFormatter.GetRegionId();
        if (isDeleted) {
            return;
        }
        ReadValue(offsetFormatter.GetOffset(), regionId, value);
    }

    size_t EstimateMemoryUse(const config::KVIndexConfigPtr& kvIndexConfig,
                             const framework::SegmentMetricsVec& segmentMetrics,
                             const index_base::SegmentTopologyInfo& targetTopoInfo) override;

private:
    // T is Timestamp0Value<offset_t> or TimestampValue<offset_t>
    typedef typename Traits::ValueType T;
    typedef typename Traits::template FileIterator<true> Iterator;
    static const size_t INIT_BUFFER_SIZE = 4096; // 4K

private:
    bool OpenKey(const file_system::DirectoryPtr& kvDir);
    bool OpenValue(const file_system::DirectoryPtr& kvDir, const config::KVIndexConfigPtr& kvIndexConfig);
    void ReadValue(offset_t offset, regionid_t regionId, autil::StringView& value);

private:
    file_system::FileReaderPtr mKeyFileReader;
    file_system::FileReaderPtr mValueFileReader;
    Iterator mIterator;
    size_t mBufferSize;
    char* mBuffer;
    offset_t mPrevOffset; // for check order
    std::vector<int32_t> mFixValueLens;

private:
    IE_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, HashTableVarSegmentIterator);

template <typename Traits>
inline bool HashTableVarSegmentIterator<Traits>::Open(const config::IndexPartitionSchemaPtr& schema,
                                                      const index_base::SegmentData& segmentData)
{
    config::KVIndexConfigPtr kvIndexConfig = CreateDataKVIndexConfig(schema);
    file_system::DirectoryPtr directory = segmentData.GetDirectory();
    file_system::DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, true);
    file_system::DirectoryPtr kvDir = indexDir->GetDirectory(kvIndexConfig->GetIndexName(), true);

    if (!OpenKey(kvDir)) {
        return false;
    }
    if (!OpenValue(kvDir, kvIndexConfig)) {
        return false;
    }

    mFixValueLens.reserve(schema->GetRegionCount());
    for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++) {
        config::KVIndexConfigPtr kvConfig =
            DYNAMIC_POINTER_CAST(config::KVIndexConfig, schema->GetIndexSchema(id)->GetPrimaryKeyIndexConfig());
        mFixValueLens.push_back(kvConfig->GetValueConfig()->GetFixedLength());
    }
    return true;
}

template <typename Traits>
inline bool HashTableVarSegmentIterator<Traits>::OpenKey(const file_system::DirectoryPtr& kvDir)
{
    mKeyFileReader =
        kvDir->CreateFileReader(KV_KEY_FILE_NAME, file_system::ReaderOption::CacheFirst(file_system::FSOT_BUFFERED));
    if (!mKeyFileReader) {
        IE_LOG(ERROR, "open file failed, file[%s], FSOpenType [%d]", mKeyFileReader->DebugString().c_str(),
               mKeyFileReader->GetOpenType());
        return false;
    }
    if (!mIterator.Init(mKeyFileReader)) {
        IE_LOG(ERROR, "create hash file iterator failed. file[%s]", mKeyFileReader->DebugString().c_str());
        return false;
    }
    mIterator.SortByValue();
    return true;
}

template <typename Traits>
inline bool HashTableVarSegmentIterator<Traits>::OpenValue(const file_system::DirectoryPtr& kvDir,
                                                           const config::KVIndexConfigPtr& kvIndexConfig)
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

template <typename Traits>
inline void HashTableVarSegmentIterator<Traits>::ReadValue(offset_t offset, regionid_t regionId,
                                                           autil::StringView& value)
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
        size_t retLen = mValueFileReader->Read(mBuffer, sizeof(uint8_t), offset);
        if (retLen != sizeof(uint8_t)) {
            IE_LOG(ERROR, "read encodeCount first bytes failed from file[%s]", mValueFileReader->DebugString().c_str());
            return;
        }
        encodeCountLen = common::VarNumAttributeFormatter::GetEncodedCountFromFirstByte(*mBuffer);
        assert(encodeCountLen < mBufferSize);
        retLen = mValueFileReader->Read(mBuffer + sizeof(uint8_t), encodeCountLen - 1, offset + sizeof(uint8_t));
        if (retLen != encodeCountLen - 1) {
            IE_LOG(ERROR, "read encodeCount from file[%s]", mValueFileReader->DebugString().c_str());
            return;
        }

        bool isNull = false;
        uint32_t itemCount = common::VarNumAttributeFormatter::DecodeCount(mBuffer, encodeCountLen, isNull);
        assert(!isNull);

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

    if (itemLen != mValueFileReader->Read(mBuffer + encodeCountLen, itemLen, offset + encodeCountLen)) {
        IE_LOG(ERROR, "read value from file[%s]", mValueFileReader->DebugString().c_str());
        return;
    }
    value.reset(mBuffer, dataLen); // pack value not actual value
}

template <typename Traits>
inline size_t
HashTableVarSegmentIterator<Traits>::EstimateMemoryUse(const config::KVIndexConfigPtr& kvIndexConfig,
                                                       const framework::SegmentMetricsVec& segmentMetrics,
                                                       const index_base::SegmentTopologyInfo& targetTopoInfo)
{
    uint64_t maxKeyCount = 0;
    const std::string& groupName = index_base::SegmentMetricsUtil::GetColumnGroupName(targetTopoInfo.columnIdx);
    for (size_t i = 0; i < segmentMetrics.size(); ++i) {
        uint64_t keyCount = segmentMetrics[i]->Get<size_t>(groupName, KV_KEY_COUNT);
        if (maxKeyCount < keyCount) {
            maxKeyCount = keyCount;
        }
    }
    return Iterator::EstimateMemoryUse(maxKeyCount) + file_system::ReaderOption ::DEFAULT_BUFFER_SIZE * 2;
}
}} // namespace indexlib::index
