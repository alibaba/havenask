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

#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, ShardPartitioner);
DECLARE_REFERENCE_CLASS(index_base, BuildingSegmentData);

namespace indexlib { namespace partition {

class MultiShardingSegmentWriter : public index_base::SegmentWriter
{
public:
    MultiShardingSegmentWriter(const config::IndexPartitionSchemaPtr& schema,
                               const config::IndexPartitionOptions& options,
                               const std::vector<index_base::SegmentWriterPtr>& shardingWriters);

    ~MultiShardingSegmentWriter();

public:
    void Init(index_base::BuildingSegmentData& segmentData, const std::shared_ptr<framework::SegmentMetrics>& metrics,
              const util::QuotaControlPtr& buildMemoryQuotaControler,
              const util::BuildResourceMetricsPtr& buildResourceMetrics) override;

    MultiShardingSegmentWriter* CloneWithNewSegmentData(index_base::BuildingSegmentData& segmentData,
                                                        bool isShared) const override;
    bool AddDocument(const document::DocumentPtr& doc) override;
    bool IsDirty() const override;
    bool NeedForceDump() override;
    void EndSegment() override;
    void CreateDumpItems(const file_system::DirectoryPtr& directory,
                         std::vector<std::unique_ptr<common::DumpItem>>& dumpItems) override;
    void CollectSegmentMetrics() override;
    index_base::InMemorySegmentReaderPtr CreateInMemSegmentReader() override;

    const index_base::SegmentInfoPtr& GetSegmentInfo() const override;
    InMemorySegmentModifierPtr GetInMemorySegmentModifier() override;

private:
    MultiShardingSegmentWriter(const MultiShardingSegmentWriter& other, index_base::BuildingSegmentData& segmentData,
                               bool isShared);
    index::ShardPartitioner* Create(const config::IndexPartitionSchemaPtr& schema, uint32_t shardCount);

private:
    index::ShardPartitionerPtr mPartitioner;
    std::vector<index_base::SegmentWriterPtr> mShardingWriters;
    index_base::SegmentInfoPtr mCurrentSegmentInfo;
    index_base::BuildingSegmentData mSegmentData;
    bool mNeedForceDump;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiShardingSegmentWriter);
}} // namespace indexlib::partition
