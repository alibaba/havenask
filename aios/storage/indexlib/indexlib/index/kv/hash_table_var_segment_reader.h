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
#ifndef __INDEXLIB_HASH_TABLE_VAR_SEGMENT_READER_H
#define __INDEXLIB_HASH_TABLE_VAR_SEGMENT_READER_H

#include <memory>

#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common/field_format/pack_attribute/plain_format_encoder.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/SwapMmapFileReader.h"
#include "indexlib/index/kv/kv_segment_offset_reader.h"
#include "indexlib/index/kv/kv_segment_reader_impl_base.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class HashTableVarSegmentReader final : public KVSegmentReaderImplBase
{
public:
    HashTableVarSegmentReader()
    {
        mValueBaseAddr = NULL;
        mUseBaseAddr = false;
        mIsMultiRegion = false;
        mHasFixValueLen = false;
        mFixValueLen = -1;
        mCodegenHasBaseAddr = false;
    }
    ~HashTableVarSegmentReader() {}

public:
    void Open(const config::KVIndexConfigPtr& kvIndexConfig, const index_base::SegmentData& segmentData) override final;

    FL_LAZY(bool)
    Get(const KVIndexOptions* options, keytype_t key, autil::StringView& value, uint64_t& ts, bool& isDeleted,
        autil::mem_pool::Pool* pool = NULL, KVMetricsCollector* collector = nullptr) const override final;
    void TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id) override;

private:
    file_system::FileReaderPtr CreateValueFileReader(const file_system::DirectoryPtr& directory,
                                                     const config::KVIndexConfigPtr& kvIndexConfig,
                                                     std::string fileName, bool useSwapMmapFile = false);

    bool OpenValue(const file_system::DirectoryPtr& kvDir, const config::KVIndexConfigPtr& kvIndexConfig,
                   const index_base::SegmentData& segmentData);

    inline FL_LAZY(bool) GetValue(const KVIndexOptions* options, autil::StringView& value, offset_t offset,
                                  autil::mem_pool::Pool* pool, KVMetricsCollector* collector) const __ALWAYS_INLINE;

    inline bool GetValueFromAddress(const KVIndexOptions* options, autil::StringView& value, offset_t offset,
                                    autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

    inline FL_LAZY(bool)
        GetValueFromFileReader(const KVIndexOptions* options, autil::StringView& value, offset_t offset,
                               autil::mem_pool::Pool* pool, KVMetricsCollector* collector) const __ALWAYS_INLINE;

    std::string GetValueFileName(const index_base::SegmentData& segmentData) const;

    bool UseSwapMMapFile(const file_system::DirectoryPtr& dir, const config::KVIndexConfigPtr& kvIndexConfig,
                         const index_base::SegmentData& segmentData) const;
    bool doCollectAllCode() override;

private:
    typedef KVSegmentOffsetReader OffsetReaderType;
    OffsetReaderType mOffsetReader;
    char* mValueBaseAddr;
    file_system::FileReaderPtr mValueFileReader;

    // for codegen info
    bool mUseBaseAddr;
    bool mIsMultiRegion;
    bool mHasFixValueLen;
    int64_t mFixValueLen;

    bool mCodegenHasBaseAddr;

private:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////
inline file_system::FileReaderPtr
HashTableVarSegmentReader::CreateValueFileReader(const file_system::DirectoryPtr& directory,
                                                 const config::KVIndexConfigPtr& kvIndexConfig, std::string fileName,
                                                 bool useSwapMmapFile)
{
    if (useSwapMmapFile) {
        // building swap file
        return directory->CreateFileReader(fileName, file_system::ReaderOption::SwapMmap());
    }

    if (directory->IsExist(fileName)) {
        if (kvIndexConfig->GetUseSwapMmapFile() && directory->GetStorageType(fileName) == file_system::FSST_MEM) {
            // value with swap file dump to built segment
            return directory->CreateFileReader(fileName, file_system::ReaderOption::SwapMmap());
        }
        return directory->CreateFileReader(fileName, file_system::FSOT_LOAD_CONFIG);
    }
    return file_system::FileReaderPtr();
}

inline bool HashTableVarSegmentReader::GetValueFromAddress(const KVIndexOptions* options, autil::StringView& value,
                                                           offset_t offset, autil::mem_pool::Pool* pool) const
{
    if (!mIsMultiRegion) {
        if (mHasFixValueLen) {
            assert(!options->plainFormatEncoder);
            autil::MultiChar multiChar(mValueBaseAddr + offset, mFixValueLen);
            value = {multiChar.data(), multiChar.size()};
        } else {
            autil::MultiChar multiChar(mValueBaseAddr + offset);
            value = {multiChar.data(), multiChar.size()};
            if (options->plainFormatEncoder) {
                return options->plainFormatEncoder->Decode(value, pool, value);
            }
        }
        return true;
    }

    assert(!options->plainFormatEncoder);
    if (options->fixedValueLen > 0) {
        autil::MultiChar multiChar(mValueBaseAddr + offset, options->fixedValueLen);
        value = {multiChar.data(), multiChar.size()};
    } else {
        autil::MultiChar multiChar(mValueBaseAddr + offset);
        value = {multiChar.data(), multiChar.size()};
    }
    return true;
}

inline FL_LAZY(bool) HashTableVarSegmentReader::GetValueFromFileReader(const KVIndexOptions* options,
                                                                       autil::StringView& value, offset_t offset,
                                                                       autil::mem_pool::Pool* pool,
                                                                       KVMetricsCollector* collector) const
{
    // read from file
    // read len
    size_t encodeCountLen = 0;
    size_t itemLen = 0;

    file_system::ReadOption option;
    option.blockCounter = collector ? collector->GetBlockCounter() : nullptr;
    option.advice = file_system::IO_ADVICE_LOW_LATENCY;

    if (options->fixedValueLen > 0) {
        itemLen = options->fixedValueLen;
    } else {
        uint8_t lenBuffer[8];
        auto ret = FL_COAWAIT mValueFileReader->ReadAsyncCoro(lenBuffer, 1, offset, option);
        if (!ret.OK() || 1 != ret.Value()) {
            AUTIL_LOG(ERROR, "read encodeCount first bytes failed from file[%s]",
                      mValueFileReader->DebugString().c_str());
            FL_CORETURN false;
        }
        encodeCountLen = common::VarNumAttributeFormatter::GetEncodedCountFromFirstByte(lenBuffer[0]);
        assert(encodeCountLen <= sizeof(lenBuffer));
        ret = FL_COAWAIT mValueFileReader->ReadAsyncCoro(lenBuffer + 1, encodeCountLen - 1, offset + 1, option);
        if (!ret.OK() || (encodeCountLen - 1) != ret.Value()) {
            AUTIL_LOG(ERROR, "read encodeCount from file[%s]", mValueFileReader->DebugString().c_str());
            FL_CORETURN false;
        }

        bool isNull = false;
        uint32_t itemCount =
            common::VarNumAttributeFormatter::DecodeCount((const char*)lenBuffer, encodeCountLen, isNull);
        assert(!isNull);
        itemLen = itemCount * sizeof(char);
    }
    char* buffer = (char*)pool->allocate(itemLen);
    auto ret = FL_COAWAIT mValueFileReader->ReadAsyncCoro(buffer, itemLen, offset + encodeCountLen, option);
    if (!ret.OK() || itemLen != ret.Value()) {
        AUTIL_LOG(ERROR, "read value from file[%s]", mValueFileReader->DebugString().c_str());
        FL_CORETURN false;
    }

    value = {buffer, itemLen};
    if (options->plainFormatEncoder && !options->plainFormatEncoder->Decode(value, pool, value)) {
        AUTIL_LOG(ERROR, "decode plain format from file [%s]", mValueFileReader->DebugString().c_str());
        FL_CORETURN false;
    }
    FL_CORETURN true;
}

inline FL_LAZY(bool) HashTableVarSegmentReader::GetValue(const KVIndexOptions* options, autil::StringView& value,
                                                         offset_t offset, autil::mem_pool::Pool* pool,
                                                         KVMetricsCollector* collector) const
{
    if (mUseBaseAddr) {
        FL_CORETURN GetValueFromAddress(options, value, offset, pool);
    } else {
        FL_CORETURN FL_COAWAIT GetValueFromFileReader(options, value, offset, pool, collector);
    }
}

inline FL_LAZY(bool) HashTableVarSegmentReader::Get(const KVIndexOptions* options, keytype_t key,
                                                    autil::StringView& value, uint64_t& ts, bool& isDeleted,
                                                    autil::mem_pool::Pool* pool, KVMetricsCollector* collector) const
{
    offset_t offset = 0;
    regionid_t indexRegionId = INVALID_REGIONID;
    if (!FL_COAWAIT mOffsetReader.Find(key, isDeleted, offset, indexRegionId, ts, collector, pool)) {
        FL_CORETURN false;
    }
    if (mIsMultiRegion) {
        regionid_t regionId = options->GetRegionId();
        if (regionId != INVALID_REGIONID && regionId != indexRegionId) {
            IE_LOG(ERROR, "Lookup Key [%lu] not match regionId [%d:%d]", key, regionId, indexRegionId);
            FL_CORETURN false;
        }
    }

    if (!isDeleted) {
        FL_CORETURN FL_COAWAIT GetValue(options, value, offset, pool, collector);
    }
    FL_CORETURN true;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_HASH_TABLE_VAR_SEGMENT_READER_H
