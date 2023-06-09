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
#ifndef __INDEXLIB_HASH_TABLE_FIX_MERGE_WRITER_H
#define __INDEXLIB_HASH_TABLE_FIX_MERGE_WRITER_H

#include <memory>

#include "autil/ConstString.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common/hash_table/hash_table_options.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/hash_table_fix_writer.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_merge_writer.h"
#include "indexlib/index_base/index_meta/segment_metrics_util.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Status.h"

namespace indexlib { namespace index {

class HashTableFixMergeWriter final : public KVMergeWriter
{
public:
    HashTableFixMergeWriter() : mUseCompactBucket(false) {}
    ~HashTableFixMergeWriter() {}

public:
    void Init(const file_system::DirectoryPtr& segmentDirectory, const file_system::DirectoryPtr& indexDirectory,
              const config::IndexPartitionSchemaPtr& mSchema, const config::KVIndexConfigPtr& kvIndexConfig,
              const KVFormatOptionsPtr& kvOptions, bool needStorePKeyValue, autil::mem_pool::PoolBase* pool,
              const framework::SegmentMetricsVec& segmentMetrics,
              const index_base::SegmentTopologyInfo& targetTopoInfo) override final;

    bool AddIfNotExist(const keytype_t& key, const autil::StringView& keyRawValue, const autil::StringView& value,
                       uint32_t timestamp, bool isDeleted, regionid_t regionId) final;

    void OptimizeDump() override final;
    void FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                            const std::string& groupName) override final;

    size_t EstimateMemoryUse(const config::KVIndexConfigPtr& kvIndexConfig, const KVFormatOptionsPtr& kvOptions,
                             const framework::SegmentMetricsVec& segmentMetrics,
                             const index_base::SegmentTopologyInfo& targetTopoInfo) const override final;

private:
    static uint64_t EstimateMaxKeyCount(const framework::SegmentMetricsVec& segmentMetrics,
                                        const index_base::SegmentTopologyInfo& targetTopoInfo);

private:
    // T is Timestamp0Value<fieldtype> or TimestampValue<fieldtype>
    typedef common::HashTableOptions Options;

private:
    std::unique_ptr<common::HashTableBase> mHashTable;
    std::unique_ptr<common::ValueUnpacker> mValueUnpacker;
    file_system::FileWriterPtr mFileWriter;
    bool mUseCompactBucket;

private:
    IE_LOG_DECLARE();
};
}}     // namespace indexlib::index
#endif //__INDEXLIB_HASH_TABLE_FIX_MERGE_WRITER_H
