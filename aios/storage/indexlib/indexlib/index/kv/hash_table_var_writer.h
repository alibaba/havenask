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

#include "autil/StringView.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/document/document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/hash_table_var_creator.h"
#include "indexlib/index/kv/kv_common.h"
#include "indexlib/index/kv/kv_format_options.h"
#include "indexlib/index/kv/kv_writer.h"
#include "indexlib/index/kv/multi_region_info.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/ValueWriter.h"

DECLARE_REFERENCE_CLASS(common, PackAttributeFormatter);

namespace indexlib { namespace index {

class HashTableVarWriter final : public KVWriter
{
public:
    HashTableVarWriter() : mValueMemoryUseThreshold(0), mMemoryRatio(DEFAULT_KEY_VALUE_MEM_RATIO), mIsOnline(true) {}

    ~HashTableVarWriter() { RemoveValueFile(); }

public:
    void Init(const config::KVIndexConfigPtr& kvIndexConfig, const file_system::DirectoryPtr& segmentDir, bool isOnline,
              uint64_t maxMemoryUse,
              const std::shared_ptr<framework::SegmentGroupMetrics>& groupMetrics) override final;

    bool Add(const keytype_t& key, const autil::StringView& value, uint32_t timestamp, bool isDeleted,
             regionid_t regionId) override final;

    void Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* pool) override final;

    void FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                            const std::string& groupName) override final;

    size_t GetDumpTempMemUse() const override final
    {
        return NeedCompress() ? mKVConfig->GetIndexPreference().GetValueParam().GetFileCompressBufferSize() : 0;
    }

    // NOTE: memory allocated by file system
    size_t GetDumpFileExSize() const override final
    {
        size_t valueSize = mValueWriter.GetUsedBytes();
        if (NeedCompress()) {
            valueSize *= this->mCompressRatio;
        }
        return valueSize;
    }

public:
    void SetHashKeyValueMap(std::map<keytype_t, autil::StringView>* hashKeyValueMap) override
    {
        mHashKeyValueMap = hashKeyValueMap;
    }
    void SetAttributeWriter(index::AttributeWriterPtr& writer) override { mAttributeWriter = writer; }

public:
    bool IsFull() const override final { return IsKeyFull() || IsValueFull(); }

    size_t GetMemoryUse() const override final
    {
        /* discard old NOTE: memory allocated by file system, so return 0 here. */
        // new NOTE: tmp in-memory value file resize to 0, used as writer memory
        size_t valueSize = 0;
        if (!mKVConfig->GetUseSwapMmapFile()) {
            valueSize = mValueWriter.GetUsedBytes();
        }
        return valueSize + mHashTable->BuildAssistantMemoryUse();
    }

public:
    // public for HashTableVarMergeWriter<T>
    // T is Timestamp0Value<offset_t> or TimestampValue<offset_t>

    static void FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                   const std::string& groupName, const common::HashTableBase& hashTable,
                                   size_t valueMemUse, double memoryRatio);
    static std::string GetTmpValueFileName() { return std::string(KV_VALUE_FILE_NAME) + ".tmp"; }

private:
    typedef util::ValueWriter<offset_t> ValueWriter;
    static bool Add(const keytype_t& key, const autil::StringView& value, uint32_t timestamp, bool isDeleted,
                    regionid_t regionId, common::HashTableBase& hashTable, common::ValueUnpacker& valueUnpacker,
                    KVVarOffsetFormatterBase& mOffsetFormatter, ValueWriter& valueWriter);

    static double CalculateMemoryRatio(const std::shared_ptr<framework::SegmentGroupMetrics>& groupMetrics,
                                       uint64_t maxMemoryUse);
    file_system::FileWriterPtr CreateValueFile(int64_t& maxMemoryUse);

    void RemoveValueFile();
    void IncRegionInfoCount(regionid_t regionId, uint32_t count);

private:
    bool IsKeyFull() const { return mHashTable->IsFull(); }
    bool IsValueFull() const { return mValueWriter.GetUsedBytes() > mValueMemoryUseThreshold; }

    bool NeedCompress() const
    {
        const config::KVIndexPreference::ValueParam& valueParam = mKVConfig->GetIndexPreference().GetValueParam();
        return valueParam.EnableFileCompress();
    }

private:
    static constexpr double MAX_VALUE_MEMORY_USE_RATIO = 0.99;
    static constexpr double DEFAULT_KEY_VALUE_MEM_RATIO = 0.01;
    static constexpr double NEW_MEMORY_RATIO_WEIGHT = 0.7;

private:
    std::unique_ptr<common::HashTableBase> mHashTable;
    std::unique_ptr<common::HashTableFileIterator> mHashTableFileIterator;
    std::unique_ptr<common::ValueUnpacker> mValueUnpacker;
    std::unique_ptr<KVVarOffsetFormatterBase> mOffsetFormatter;
    ValueWriter mValueWriter;
    config::KVIndexConfigPtr mKVConfig;
    common::PackAttributeFormatterPtr mPackAttrFormatter;
    file_system::DirectoryPtr mSegmentDir;
    file_system::DirectoryPtr mKVDir;
    file_system::FileWriterPtr mKeyFile;
    file_system::FileWriterPtr mValueFile;
    uint64_t mValueMemoryUseThreshold;
    double mMemoryRatio;
    bool mIsOnline;
    MultiRegionInfoPtr mRegionInfo;

    // for store pkey raw value
    docid_t mCurrentDocId = 0;
    index::AttributeWriterPtr mAttributeWriter;
    std::map<keytype_t, autil::StringView>* mHashKeyValueMap;

    friend class KVWriterCreatorTest;

private:
    friend class HashTableVarWriterTest;
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
