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
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentQueue);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentContainer);
DECLARE_REFERENCE_CLASS(partition, BuildingPartitionData);
DECLARE_REFERENCE_CLASS(partition, CustomPartitionData);
DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);
DECLARE_REFERENCE_CLASS(util, PartitionMemoryQuotaController);

namespace indexlibv2::framework {
struct VersionDeployDescription;
typedef std::shared_ptr<VersionDeployDescription> VersionDeployDescriptionPtr;
} // namespace indexlibv2::framework

namespace indexlib { namespace partition {

struct BuildingPartitionParam {
    BuildingPartitionParam(const config::IndexPartitionOptions& options, const config::IndexPartitionSchemaPtr& schema,
                           const util::PartitionMemoryQuotaControllerPtr& memController,
                           const DumpSegmentContainerPtr& container, const util::CounterMapPtr& counterMap,
                           const plugin::PluginManagerPtr& pluginManager, util::MetricProviderPtr metricProvider,
                           document::SrcSignature srcSignature)
        : options(options)
        , schema(schema)
        , memController(memController)
        , container(container)
        , counterMap(counterMap)
        , pluginManager(pluginManager)
        , metricProvider(metricProvider)
        , srcSignature(srcSignature)
    {
    }
    const config::IndexPartitionOptions& options;
    const config::IndexPartitionSchemaPtr& schema;
    const util::PartitionMemoryQuotaControllerPtr& memController;
    const DumpSegmentContainerPtr& container;
    const util::CounterMapPtr& counterMap;
    const plugin::PluginManagerPtr& pluginManager;
    util::MetricProviderPtr metricProvider;
    document::SrcSignature srcSignature;
};

class PartitionDataCreator
{
public:
    PartitionDataCreator();
    ~PartitionDataCreator();

public:
    // by default:
    // use file system's root as partition root
    static BuildingPartitionDataPtr
    CreateBuildingPartitionData(const BuildingPartitionParam& param, const file_system::IFileSystemPtr& fileSystem,
                                index_base::Version version = index_base::Version(INVALID_VERSIONID),
                                const std::string& dir = "",
                                const index_base::InMemorySegmentPtr& inMemSegment = index_base::InMemorySegmentPtr());

    // same as CreateBuildingPartitionData, but do not modify old in mem segment
    static BuildingPartitionDataPtr
    CreateBranchBuildingPartitionData(const BuildingPartitionParam& param,
                                      const index_base::SegmentDirectoryPtr& segDir,
                                      const index_base::Version& ondiskVersion,
                                      const index_base::InMemorySegmentPtr& inMemSegment, bool sharedDeletionMap);

    static CustomPartitionDataPtr CreateCustomPartitionData(
        const file_system::IFileSystemPtr& fileSystem, const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options, const util::PartitionMemoryQuotaControllerPtr& memController,
        const util::BlockMemoryQuotaControllerPtr& tableWriterMemController, index_base::Version version,
        const indexlibv2::framework::VersionDeployDescriptionPtr& versionDpDesc, util::MetricProviderPtr metricProvider,
        const util::CounterMapPtr& counterMap, const plugin::PluginManagerPtr& pluginManager,
        const DumpSegmentQueuePtr& dumpSegmentQueue, int64_t reclaimTimestamp = -1, bool ignoreInMemVersion = false);

    static BuildingPartitionDataPtr
    CreateBuildingPartitionDataWithoutInMemSegment(const BuildingPartitionParam& param,
                                                   const file_system::IFileSystemPtr& fileSystem,
                                                   index_base::Version version, const std::string& dir);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionDataCreator);
}} // namespace indexlib::partition
