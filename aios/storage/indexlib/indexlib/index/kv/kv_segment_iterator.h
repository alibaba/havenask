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
#include "indexlib/common/hash_table/hash_table_base.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/kv_common.h"
#include "indexlib/index/kv/kv_format_options.h"
#include "indexlib/index_base/index_meta/segment_metrics_util.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/ValueWriter.h"

namespace indexlib { namespace index {

class KVSegmentIterator
{
private:
    static const size_t INIT_BUFFER_SIZE = 4096; // 4K
public:
    KVSegmentIterator()
        : mIsHashTableVarSegment(false)
        , mBufferSize(0)
        , mBuffer(nullptr)
        , mPrevOffset(0)
        , mIsShortOffset(false)
    {
    }
    ~KVSegmentIterator()
    {
        if (mBuffer) {
            delete[] mBuffer;
            mBuffer = nullptr;
        }
    }

public:
    bool Open(const config::IndexPartitionSchemaPtr& schema, const KVFormatOptionsPtr& kvOptions,
              const index_base::SegmentData& segmentData, bool needRandomAccess = false);
    void Get(keytype_t& key, autil::StringView& value, uint32_t& timestamp, bool& isDeleted, regionid_t& regionId);
    void GetKey(keytype_t& key, bool& isDeleted);
    void GetValue(autil::StringView& value, uint32_t& timestamp, regionid_t& regionId, bool isDeleted);
    util::Status Find(uint64_t key, autil::StringView& value) const;
    bool Seek(int64_t offset);
    int64_t GetOffset() const;

public:
    static size_t EstimateMemoryUse(const config::KVIndexConfigPtr& kvIndexConfig, const KVFormatOptionsPtr& kvOptions,
                                    const framework::SegmentMetricsVec& segmentMetrics,
                                    const index_base::SegmentTopologyInfo& targetTopoInfo);

public:
    size_t Size() const { return mHashTableIterator->Size(); }
    bool IsValid() const { return mHashTableIterator->IsValid(); }
    void MoveToNext() { mHashTableIterator->MoveToNext(); }

private:
    bool OpenKey(const file_system::DirectoryPtr& kvDir);
    bool OpenValue(const file_system::DirectoryPtr& kvDir, const config::KVIndexConfigPtr& kvIndexConfig);
    void ReadValue(offset_t offset, regionid_t regionId, autil::StringView& value);

private:
    file_system::FileReaderPtr mKeyFileReader;
    file_system::FileReaderPtr mKeyFileReaderForHash;
    file_system::FileReaderPtr mValueFileReader;
    std::unique_ptr<common::ValueUnpacker> mUnpacker;
    std::unique_ptr<common::HashTableBase> mHashTable;
    std::unique_ptr<common::HashTableFileIterator> mHashTableIterator;
    std::unique_ptr<KVVarOffsetFormatterBase> mOffsetFormatter;

    // for hash table var segment
    bool mIsHashTableVarSegment;
    size_t mBufferSize;
    char* mBuffer;
    offset_t mPrevOffset; // for check order
    std::vector<int32_t> mFixValueLens;
    bool mIsShortOffset;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVSegmentIterator);
}} // namespace indexlib::index
