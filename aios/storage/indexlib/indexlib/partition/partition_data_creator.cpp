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
#include "indexlib/partition/partition_data_creator.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/LifecycleConfig.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_directory_creator.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/dump_segment_queue.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::common;
using namespace indexlib::plugin;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionDataCreator);

PartitionDataCreator::PartitionDataCreator() {}

PartitionDataCreator::~PartitionDataCreator() {}

BuildingPartitionDataPtr
PartitionDataCreator::CreateBuildingPartitionDataWithoutInMemSegment(const BuildingPartitionParam& param,
                                                                     const IFileSystemPtr& fileSystem,
                                                                     index_base::Version version, const string& dir)
{
    const IndexPartitionSchemaPtr& schema = param.schema;
    const IndexPartitionOptions& options = param.options;
    bool hasSub = false;
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        hasSub = true;
    }

    DirectoryPtr baseDir = Directory::Get(fileSystem);
    if (!dir.empty()) {
        baseDir = baseDir->GetDirectory(dir, true);
    }
    SegmentDirectoryPtr segDir = SegmentDirectoryCreator::Create(baseDir, version, &options, hasSub);
    if (options.IsOffline()) {
        segDir->UpdateSchemaVersionId(schema->GetSchemaVersionId());
        segDir->SetOngoingModifyOperations(options.GetOngoingModifyOperationIds());
    }

    // check index format version compatible
    IndexFormatVersion onDiskFormatVersion = segDir->GetIndexFormatVersion();
    onDiskFormatVersion.CheckCompatible(INDEX_FORMAT_VERSION);

    BuildingPartitionDataPtr partitionData(
        new BuildingPartitionData(options, schema, param.memController, param.container, param.metricProvider,
                                  param.counterMap, param.pluginManager, param.srcSignature));
    partitionData->Open(segDir, true);
    return partitionData;
}

BuildingPartitionDataPtr PartitionDataCreator::CreateBuildingPartitionData(const BuildingPartitionParam& param,
                                                                           const IFileSystemPtr& fileSystem,
                                                                           index_base::Version version,
                                                                           const string& dir,
                                                                           const InMemorySegmentPtr& inMemSegment)
{
    BuildingPartitionDataPtr partitionData =
        CreateBuildingPartitionDataWithoutInMemSegment(param, fileSystem, version, dir);
    if (inMemSegment) {
        partitionData->InheritInMemorySegment(inMemSegment);
    }
    return partitionData;
}

BuildingPartitionDataPtr PartitionDataCreator::CreateBranchBuildingPartitionData(
    const BuildingPartitionParam& param, const SegmentDirectoryPtr& lastDirectory,
    const index_base::Version& onDiskVersion, const InMemorySegmentPtr& inMemSegment, bool sharedDeletionMap)
{
    IndexFormatVersion onDiskFormatVersion = lastDirectory->GetIndexFormatVersion();
    onDiskFormatVersion.CheckCompatible(INDEX_FORMAT_VERSION);
    SegmentDirectoryPtr newDirectory(lastDirectory->Clone());
    newDirectory->Reopen(onDiskVersion);
    BuildingPartitionDataPtr partitionData(new BuildingPartitionData(param.options, param.schema, param.memController,
                                                                     param.container, param.metricProvider,
                                                                     util::CounterMapPtr(), param.pluginManager));
    partitionData->Open(newDirectory, sharedDeletionMap);
    if (inMemSegment) {
        partitionData->SwitchPrivateInMemSegment(inMemSegment);
    }
    return partitionData;
}

CustomPartitionDataPtr PartitionDataCreator::CreateCustomPartitionData(
    const IFileSystemPtr& fileSystem, const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
    const PartitionMemoryQuotaControllerPtr& memController,
    const BlockMemoryQuotaControllerPtr& tableWriterMemController, index_base::Version version,
    const indexlibv2::framework::VersionDeployDescriptionPtr& versionDpDesc, util::MetricProviderPtr metricProvider,
    const CounterMapPtr& counterMap, const PluginManagerPtr& pluginManager, const DumpSegmentQueuePtr& dumpSegmentQueue,
    int64_t reclaimTimestamp, bool ignoreInMemVersion)
{
    auto baseDir = Directory::Get(fileSystem);
    SegmentDirectoryPtr segDir;
    if (ignoreInMemVersion) {
        std::shared_ptr<LifecycleConfig> lifecycleConfig;
        if (options.IsOnline()) {
            lifecycleConfig = std::make_shared<LifecycleConfig>(options.GetOnlineConfig().GetLifecycleConfig());
        }
        segDir.reset(new SegmentDirectory(lifecycleConfig));
        segDir->Init(baseDir, version, false);
    } else {
        segDir = SegmentDirectoryCreator::Create(baseDir, version, &options, false);
    }

    segDir->SetVersionDeployDescription(versionDpDesc);
    CustomPartitionDataPtr partitionData(new CustomPartitionData(options, schema, memController,
                                                                 tableWriterMemController, metricProvider, counterMap,
                                                                 pluginManager, dumpSegmentQueue, reclaimTimestamp));
    if (!partitionData->Open(segDir)) {
        return CustomPartitionDataPtr();
    }
    return partitionData;
}

}} // namespace indexlib::partition
