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
#include "indexlib/common/hash_table/hash_table_base.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_format_options.h"
#include "indexlib/index/kv/kv_writer.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, AttributeWriter);

namespace indexlib { namespace index {

// T is Timestamp0Value<offset_t> or TimestampValue<>
class HashTableFixWriter final : public KVWriter
{
public:
    HashTableFixWriter() : mIsOnline(true) {}
    ~HashTableFixWriter() {}

public:
    void Init(const config::KVIndexConfigPtr& kvIndexConfig, const file_system::DirectoryPtr& segmentDir, bool isOnline,
              uint64_t maxMemoryUse,
              const std::shared_ptr<framework::SegmentGroupMetrics>& groupMetrics) override final;

    bool Add(const keytype_t& key, const autil::StringView& value, uint32_t timestamp, bool isDeleted,
             regionid_t regionId) override final;

    void Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* pool) override final;

    void FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                            const std::string& groupName) override final;

public:
    bool IsFull() const override final { return mHashTable->IsFull(); }
    size_t GetMemoryUse() const override final
    {
        // NOTE: memory allocated by file system, so return 0 here.
        return mHashTable->BuildAssistantMemoryUse();
    }

private:
public:
    // public for HashTableVarWriter<>
    static void FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                   const std::string& groupName, const common::HashTableBase& hashtable);
    static bool Add(const keytype_t& key, const autil::StringView& value, uint32_t timestamp, bool isDeleted,
                    regionid_t regionId, common::HashTableBase& hashTable, common::ValueUnpacker& valueUnpacker);

public:
    void SetHashKeyValueMap(std::map<keytype_t, autil::StringView>* hashKeyValueMap) override
    {
        mHashKeyValueMap = hashKeyValueMap;
    }
    void SetAttributeWriter(index::AttributeWriterPtr& writer) override { mAttributeWriter = writer; }

private:
    std::unique_ptr<common::HashTableBase> mHashTable;
    std::unique_ptr<common::ValueUnpacker> mValueUnpacker;
    std::unique_ptr<common::HashTableFileIterator> mHashTableFileIterator;
    config::KVIndexConfigPtr mKVConfig;
    file_system::DirectoryPtr mSegmentDir;
    file_system::DirectoryPtr mKVDir;
    file_system::FileWriterPtr mInMemFile;
    bool mIsOnline;

    // for store pkey raw value
    docid_t mCurrentDocId = 0;
    index::AttributeWriterPtr mAttributeWriter;
    std::map<keytype_t, autil::StringView>* mHashKeyValueMap;

    friend class KVWriterCreatorTest;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
