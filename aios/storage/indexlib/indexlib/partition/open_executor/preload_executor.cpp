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
#include "indexlib/partition/open_executor/preload_executor.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/online_config.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index/normal/attribute/virtual_attribute_data_appender.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/open_executor/open_executor.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/util/memory_control/MemoryReserver.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PreloadExecutor);

PreloadExecutor::PreloadExecutor() {}

PreloadExecutor::~PreloadExecutor() {}

bool PreloadExecutor::ReserveMem(ExecutorResource& resource, const MemoryReserverPtr& memReserver)
{
    DirectoryPtr rootDirectory = resource.mPartitionDataHolder.Get()->GetRootDirectory();
    size_t incExpandMemSizeInBytes = resource.mResourceCalculator->EstimateDiffVersionLockSizeWithoutPatch(
        resource.mSchema, rootDirectory, resource.mIncVersion, resource.mLoadedIncVersion);

    IE_LOG(INFO, "memory : preload need memory [%lu B], current free quota [%ld B]", incExpandMemSizeInBytes,
           memReserver->GetFreeQuota());
    if (!memReserver->Reserve(incExpandMemSizeInBytes)) {
        IE_LOG(WARN, "Preload fail, incExpandMemSize [%luB] over FreeQuota [%ldB]", incExpandMemSizeInBytes,
               memReserver->GetFreeQuota());
        return false;
    }
    return true;
}

bool PreloadExecutor::Execute(ExecutorResource& resource)
{
    util::ScopeLatencyReporter scopeTime(resource.mOnlinePartMetrics.GetpreloadLatencyMetric().get());
    IE_LOG(INFO, "Begin Preload version [%d]", resource.mIncVersion.GetVersionId());
    MemoryReserverPtr memReserver = resource.mFileSystem->CreateMemoryReserver("pre_load");

    if (!ReserveMem(resource, memReserver)) {
        return false;
    }

    bool needDeletionMap = resource.mSchema->GetTableType() != tt_kkv && resource.mSchema->GetTableType() != tt_kv;
    OnDiskPartitionDataPtr incPartitionData = OnDiskPartitionData::CreateOnDiskPartitionData(
        resource.mFileSystem, resource.mSchema, resource.mIncVersion, "", needDeletionMap, resource.mPluginManager);

    VirtualAttributeDataAppender::PrepareVirtualAttrData(resource.mRtSchema, incPartitionData);

    IndexPartitionOptions options = resource.mOptions;
    options.GetOnlineConfig().disableLoadCustomizedIndex = true;

    mPreloadIncReader.reset(new OnlinePartitionReader(options, resource.mSchema, resource.mSearchCache, NULL));
    mPreloadIncReader->Open(incPartitionData);
    resource.mPreloadReader = mPreloadIncReader;
    IE_LOG(INFO, "End Preload version [%d]", resource.mIncVersion.GetVersionId());
    return true;
}

void PreloadExecutor::Drop(ExecutorResource& resource)
{
    mPreloadIncReader.reset();
    resource.mPreloadReader.reset();
}
}} // namespace indexlib::partition
