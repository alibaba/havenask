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

#include "autil/StringUtil.h"
#include "indexlib/base/Types.h"
#include "indexlib/common/hash_table/cuckoo_hash_table.h"
#include "indexlib/common/hash_table/cuckoo_hash_table_file_reader.h"
#include "indexlib/common/hash_table/dense_hash_table.h"
#include "indexlib/common/hash_table/dense_hash_table_file_reader.h"
#include "indexlib/common/hash_table/hash_table_base.h"
#include "indexlib/common/hash_table/hash_table_file_reader_base.h"
#include "indexlib/common_define.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/value_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_format_options.h"
#include "indexlib/index/kv/kv_segment_reader_impl_base.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/FloatInt8Encoder.h"
#include "indexlib/util/Fp16Encoder.h"
#include "indexlib/util/Status.h"

namespace indexlib { namespace index {

class HashTableFixSegmentReader final : public KVSegmentReaderImplBase
{
public:
    HashTableFixSegmentReader()
    {
        mHasTTL = false;
        mHasHashTableReader = false;
        mCodegenHasHashTableReader = false;
        mCodegenCompactBucket = false;
    }
    ~HashTableFixSegmentReader() {}

public:
    void Open(const config::KVIndexConfigPtr& kvIndexConfig, const index_base::SegmentData& segmentData) override final;
    FL_LAZY(bool)
    Get(const KVIndexOptions* options, keytype_t key, autil::StringView& value, uint64_t& ts, bool& isDeleted,
        autil::mem_pool::Pool* pool = NULL, KVMetricsCollector* collector = nullptr) const override final;
    void TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id) override;

private:
    using KVMap = common::KVMap;

private:
    typedef common::HashTableBase HashTable;                     // todo codegen
    typedef common::HashTableFileReaderBase HashTableFileReader; // todo codegen
    typedef std::unique_ptr<HashTable> HashTablePtr;
    typedef std::unique_ptr<HashTableFileReader> HashTableFileReaderPtr;
    typedef common::ValueUnpacker ValueUnpacker;
    typedef std::unique_ptr<ValueUnpacker> ValueUnpackerPtr; // todo codegen

    typedef common::HashTableBase CompactHashTable;                     // todo codegen
    typedef common::HashTableFileReaderBase CompactHashTableFileReader; // todo codegen
    typedef std::unique_ptr<CompactHashTable> CompactHashTablePtr;
    typedef std::unique_ptr<CompactHashTableFileReader> CompactHashTableFileReaderPtr;
    typedef common::ValueUnpacker CompactValueUnpacker;
    typedef std::unique_ptr<CompactValueUnpacker> CompactValueUnpackerPtr; // todo codegen

private:
    file_system::FileReaderPtr CreateFileReader(const file_system::DirectoryPtr& directory);
    void InnerOpen(bool useCompactBucket, const file_system::DirectoryPtr& kvDir,
                   const index_base::SegmentData& segmentData, const config::KVIndexConfigPtr& kvIndexConfig);
    bool doCollectAllCode() override;
    std::string GetTableName(KVMap& nameInfo, bool compactBucket, bool isReader);

private:
    void CollectCodegenTableName(KVMap& nameInfo);
    bool InnerCopy(autil::StringView& value, void* data, autil::mem_pool::Pool* pool) const;

private:
    HashTablePtr mHashTable;
    HashTableFileReaderPtr mHashTableFileReader;
    ValueUnpackerPtr mValueUnpacker;

    CompactHashTablePtr mCompactHashTable;
    CompactHashTableFileReaderPtr mCompactHashTableFileReader;

    file_system::FileReaderPtr mFileReader;

    // for codegen
    bool mHasTTL;
    bool mHasHashTableReader;
    bool mCompactBucket;
    std::string mHashTableName;
    std::string mHashTableFileReaderName;
    std::string mUnpackerName;

    std::string mCompactHashTableName;
    std::string mCompactHashTableFileReaderName;

    bool mCodegenHasHashTableReader;
    bool mCodegenCompactBucket;
    config::CompressTypeOption mCompressType;

    indexlibv2::CompressTypeFlag mValueType = indexlibv2::ct_other;
    size_t mValueSize = 0;

private:
    IE_LOG_DECLARE();
};
// notice: can not partial specialization for template member function in template classs

inline bool HashTableFixSegmentReader::InnerCopy(autil::StringView& value, void* data,
                                                 autil::mem_pool::Pool* pool) const
{
    using namespace indexlibv2;
    if (mValueType == ct_int8) {
        autil::StringView input((char*)data, sizeof(int8_t));
        float* rawValue = (float*)pool->allocate(sizeof(float));
        if (util::FloatInt8Encoder::Decode(mCompressType.GetInt8AbsMax(), input, (char*)(rawValue)) != 1) {
            return false;
        }
        value = {(char*)(rawValue), sizeof(float)};
        return true;
    } else if (mValueType == ct_fp16) {
        float* rawValue = (float*)pool->allocate(sizeof(float));
        autil::StringView input((char*)data, sizeof(int16_t));
        if (util::Fp16Encoder::Decode(input, (char*)(rawValue)) != 1) {
            return false;
        }
        value = {(char*)(rawValue), sizeof(float)};
        return true;
    } else {
        assert(mValueType == ct_other);
        value = {(char*)(data), mValueSize};
        return true;
    }
}

inline FL_LAZY(bool) HashTableFixSegmentReader::Get(const KVIndexOptions* options, keytype_t key,
                                                    autil::StringView& value, uint64_t& ts, bool& isDeleted,
                                                    autil::mem_pool::Pool* pool, KVMetricsCollector* collector) const
{
    autil::StringView tmpValue;

    util::Status status;
    if (!mHasHashTableReader) {
        if (!mCompactBucket) {
            status = mHashTable->Find(key, tmpValue);
        } else {
            status = mCompactHashTable->Find(key, tmpValue);
        }
    } else {
        util::BlockAccessCounter* blockCounter = collector ? collector->GetBlockCounter() : nullptr;
        if (!mCompactBucket) {
            status = FL_COAWAIT mHashTableFileReader->Find(key, tmpValue, blockCounter, pool, nullptr);
        } else {
            status = FL_COAWAIT mCompactHashTableFileReader->Find(key, tmpValue, blockCounter, pool, nullptr);
        }
    }
    if (status == util::NOT_FOUND) {
        FL_CORETURN false;
    }
    autil::StringView encodedValue;
    mValueUnpacker->Unpack(tmpValue, ts, encodedValue);
    if (status == util::DELETED) {
        isDeleted = true;
        FL_CORETURN true;
    }
    isDeleted = false;
    FL_CORETURN InnerCopy(value, (void*)encodedValue.data(), pool);
}
}} // namespace indexlib::index
