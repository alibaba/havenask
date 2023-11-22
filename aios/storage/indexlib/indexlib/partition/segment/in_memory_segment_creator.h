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
#include "indexlib/config/index_partition_options.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(framework, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index_base, BuildingSegmentData);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, PartitionSegmentIterator);
DECLARE_REFERENCE_CLASS(partition, SingleSegmentWriter);
DECLARE_REFERENCE_CLASS(partition, InMemoryPartitionData);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, PartitionMemoryQuotaController);
DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace partition {

class InMemorySegmentCreator
{
public:
    InMemorySegmentCreator(uint32_t shardingColumnNum = 1);
    ~InMemorySegmentCreator();

public:
    enum CreateOption {
        SHARED = 1,
        PRIVATE = 2,
    };
    void Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options);

    index_base::InMemorySegmentPtr Create(const InMemoryPartitionDataPtr& inMemPartData,
                                          const util::PartitionMemoryQuotaControllerPtr& memController);

    index_base::InMemorySegmentPtr Create(const InMemoryPartitionDataPtr& inMemPartData,
                                          const index_base::InMemorySegmentPtr& inMemorySegment, CreateOption option);

private:
    index_base::InMemorySegmentPtr Create(const index_base::PartitionSegmentIteratorPtr& partitionSegIter,
                                          const index_base::BuildingSegmentData& segmentData,
                                          const file_system::DirectoryPtr& segParentDirectory,
                                          const util::PartitionMemoryQuotaControllerPtr& memController,
                                          const util::CounterMapPtr& counterMap,
                                          const plugin::PluginManagerPtr& pluginManager);

    index_base::InMemorySegmentPtr Create(const index_base::BuildingSegmentData& segmentData,
                                          const file_system::DirectoryPtr& segParentDirectory,
                                          const index_base::InMemorySegmentPtr& inMemorySegment, CreateOption option);

    index_base::SegmentWriterPtr DoCreate(const index_base::PartitionSegmentIteratorPtr& partitionSegIter,
                                          index_base::BuildingSegmentData& segmentData,
                                          const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                          const util::BlockMemoryQuotaControllerPtr& segmentMemController,
                                          const util::CounterMapPtr& counterMap,
                                          const plugin::PluginManagerPtr& pluginManager);

    SingleSegmentWriterPtr CreateSingleSegmentWriter(const index_base::BuildingSegmentData& segmentData,
                                                     const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                                     const util::BlockMemoryQuotaControllerPtr& segmentMemController,
                                                     const util::CounterMapPtr& counterMap,
                                                     const plugin::PluginManagerPtr& pluginManager,
                                                     const index_base::PartitionSegmentIteratorPtr& partitionSegIter);

    index_base::InMemorySegmentPtr
    CreateInMemorySegment(const index_base::BuildingSegmentData& segmentData,
                          const index_base::SegmentWriterPtr& segmentWriter, const config::BuildConfig& buildConfig,
                          bool isSubSegment, const util::BlockMemoryQuotaControllerPtr& segmentMemController,
                          const util::CounterMapPtr& counterMap);

    index_base::SegmentWriterPtr
    CreateKKVSegmentWriter(index_base::BuildingSegmentData& segmentData,
                           const std::shared_ptr<framework::SegmentMetrics>& metrics,
                           const util::BlockMemoryQuotaControllerPtr& segmentMemController);

    index_base::SegmentWriterPtr CreateKVSegmentWriter(index_base::BuildingSegmentData& segmentData,
                                                       const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                                       const util::BlockMemoryQuotaControllerPtr& segmentMemController);

private:
    index_base::SegmentWriterPtr
    CreateShardingKKVSegmentWriter(index_base::BuildingSegmentData& segmentData,
                                   const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                   const util::BlockMemoryQuotaControllerPtr& segmentMemController);

    index_base::SegmentWriterPtr
    CreateNoShardingKKVSegmentWriter(index_base::BuildingSegmentData& segmentData,
                                     const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                     const util::BlockMemoryQuotaControllerPtr& segmentMemController);

    index_base::SegmentWriterPtr
    CreateShardingKVSegmentWriter(index_base::BuildingSegmentData& segmentData,
                                  const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                  const util::BlockMemoryQuotaControllerPtr& segmentMemController);

    index_base::SegmentWriterPtr
    CreateNoShardingKVSegmentWriter(index_base::BuildingSegmentData& segmentData,
                                    const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                    const util::BlockMemoryQuotaControllerPtr& segmentMemController);

    void UpdateSegmentMetrics(const index_base::PartitionDataPtr& partData);

    void SetSegmentInitMemUseRatio(const std::shared_ptr<framework::SegmentMetrics>& metrics, size_t initMem,
                                   const util::BlockMemoryQuotaControllerPtr& segmentMemController);

private:
    static void ExtractNonTruncateIndexNames(const config::IndexPartitionSchemaPtr& schema,
                                             std::vector<std::string>& indexNames);

    static void ExtractNonTruncateIndexNames(const config::IndexSchemaPtr& indexSchemaPtr,
                                             std::vector<std::string>& indexNames);

private:
    std::shared_ptr<framework::SegmentMetrics> mSegmentMetrics;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    uint32_t mColumnNum;

private:
    friend class InMemorySegmentTest;
    friend class InMemorySegmentCreatorTest;
    friend class InMemoryPartitionDataTest;
    friend class BuildingPartitionDataTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemorySegmentCreator);
}} // namespace indexlib::partition
