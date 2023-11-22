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

#include "autil/ConstString.h"
#include "indexlib/common/hash_table/bucket_compressor.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/kv_common.h"
#include "indexlib/index/kv/kv_merge_writer.h"
#include "indexlib/index/kv/multi_region_info.h"
#include "indexlib/index_base/index_meta/segment_metrics_util.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/ValueWriter.h"

namespace indexlib { namespace index {

class HashTableVarMergeWriter final : public KVMergeWriter
{
public:
    HashTableVarMergeWriter() : mValueFileLength(0), mAvgMemoryRatio(0.0) {}
    ~HashTableVarMergeWriter() {}

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
    static double EstimateMemoryRatio(const framework::SegmentMetricsVec& segmentMetrics,
                                      const index_base::SegmentTopologyInfo& targetTopoInfo);

    void IncRegionInfoCount(regionid_t regionId, uint32_t count);

    bool Add(const keytype_t& key, const autil::StringView& value, uint32_t timestamp, bool isDeleted,
             regionid_t regionId);
    offset_t GetValueLen() const;

private:
    std::unique_ptr<common::HashTableBase> mHashTable;
    std::unique_ptr<common::ValueUnpacker> mValueUnpacker;
    std::unique_ptr<KVVarOffsetFormatterBase> mOffsetFormatter;
    std::unique_ptr<common::BucketCompressor> mBucketCompressor;

    file_system::FileWriterPtr mKeyFileWriter;
    file_system::FileWriterPtr mValueFileWriter;
    size_t mValueFileLength;
    double mAvgMemoryRatio;
    MultiRegionInfoPtr mRegionInfo;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
