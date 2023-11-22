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
#include "indexlib/common/field_format/pack_attribute/plain_format_encoder.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/kv/kv_segment_offset_reader.h"
#include "indexlib/index/kv/kv_segment_reader_impl_base.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class HashTableCompressVarSegmentReader final : public KVSegmentReaderImplBase
{
public:
    HashTableCompressVarSegmentReader() {}
    ~HashTableCompressVarSegmentReader() {}

public:
    void Open(const config::KVIndexConfigPtr& kvIndexConfig, const index_base::SegmentData& segmentData) override final;
    FL_LAZY(bool)
    Get(const KVIndexOptions* options, keytype_t key, autil::StringView& value, uint64_t& ts, bool& isDeleted,
        autil::mem_pool::Pool* pool = NULL, KVMetricsCollector* collector = nullptr) const override final;
    void TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id) override;

private:
    bool OpenValue(const file_system::DirectoryPtr& kvDir, const config::KVIndexConfigPtr& kvIndexConfig);

    FL_LAZY(bool)
    GetCompressValue(const KVIndexOptions* options, offset_t offset, autil::StringView& value,
                     autil::mem_pool::Pool* pool, KVMetricsCollector* collector) const;
    bool doCollectAllCode() override;

private:
    typedef KVSegmentOffsetReader OffsetReaderType;
    OffsetReaderType mOffsetReader;
    file_system::CompressFileReaderPtr mCompressValueReader;

private:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////
inline FL_LAZY(bool) HashTableCompressVarSegmentReader::Get(const KVIndexOptions* options, keytype_t key,
                                                            autil::StringView& value, uint64_t& ts, bool& isDeleted,
                                                            autil::mem_pool::Pool* pool,
                                                            KVMetricsCollector* collector) const
{
    offset_t offset = 0;
    regionid_t indexRegionId = INVALID_REGIONID;
    if (!FL_COAWAIT mOffsetReader.Find(key, isDeleted, offset, indexRegionId, ts, collector, pool)) {
        FL_CORETURN false;
    }

    regionid_t regionId = options->GetRegionId();
    if (regionId != INVALID_REGIONID && regionId != indexRegionId) {
        IE_LOG(ERROR, "Lookup Key [%lu] not match regionId [%d:%d]", key, regionId, indexRegionId);
        FL_CORETURN false;
    }

    if (isDeleted) {
        FL_CORETURN true;
    }
    FL_CORETURN FL_COAWAIT GetCompressValue(options, offset, value, pool, collector);
}

inline FL_LAZY(bool) HashTableCompressVarSegmentReader::GetCompressValue(const KVIndexOptions* options, offset_t offset,
                                                                         autil::StringView& value,
                                                                         autil::mem_pool::Pool* pool,
                                                                         KVMetricsCollector* collector) const
{
    assert(mCompressValueReader);
    assert(offset < mCompressValueReader->GetUncompressedFileLength());
    file_system::CompressFileReader* fileReader = mCompressValueReader->CreateSessionReader(pool);
    assert(fileReader);
    file_system::CompressFileReaderGuard compressFileGuard(fileReader, pool);

    file_system::ReadOption option;
    option.blockCounter = collector ? collector->GetBlockCounter() : nullptr;
    option.advice = file_system::IO_ADVICE_LOW_LATENCY;

    size_t encodeCountLen = 0;
    size_t itemLen = 0;
    if (options->fixedValueLen > 0) {
        itemLen = options->fixedValueLen;
    } else {
        // read encodeCount
        uint8_t buffer[5];
        size_t retLen = (FL_COAWAIT fileReader->ReadAsyncCoro(buffer, sizeof(uint8_t), offset, option)).GetOrThrow();
        if (retLen != sizeof(uint8_t)) {
            IE_LOG(ERROR, "read encodeCount first bytes failed from file[%s]", fileReader->DebugString().c_str());
            FL_CORETURN false;
        }
        encodeCountLen = common::VarNumAttributeFormatter::GetEncodedCountFromFirstByte(*buffer);
        retLen = (FL_COAWAIT fileReader->ReadAsyncCoro(buffer + sizeof(uint8_t), encodeCountLen - 1,
                                                       offset + sizeof(uint8_t), option))
                     .GetOrThrow();
        if (retLen != encodeCountLen - 1) {
            IE_LOG(ERROR, "read encodeCount from file[%s]", fileReader->DebugString().c_str());
            FL_CORETURN false;
        }

        bool isNull = false;
        uint32_t itemCount = common::VarNumAttributeFormatter::DecodeCount((const char*)buffer, encodeCountLen, isNull);
        assert(!isNull);
        // read value
        itemLen = itemCount * sizeof(char);
    }
    assert(pool);
    char* poolBuf = (char*)pool->allocate(itemLen);
    size_t retLen =
        (FL_COAWAIT fileReader->ReadAsyncCoro(poolBuf, itemLen, offset + encodeCountLen, option)).GetOrThrow();
    if (retLen != itemLen) {
        IE_LOG(ERROR, "read value from file[%s]", fileReader->DebugString().c_str());
        FL_CORETURN false;
    }

    value = {poolBuf, itemLen};
    if (options->plainFormatEncoder && !options->plainFormatEncoder->Decode(value, pool, value)) {
        IE_LOG(ERROR, "decode plain format from file[%s] error", fileReader->DebugString().c_str());
        FL_CORETURN false;
    }
    FL_CORETURN true;
}
}} // namespace indexlib::index
