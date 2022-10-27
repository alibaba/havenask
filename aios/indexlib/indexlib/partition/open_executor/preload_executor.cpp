#include "indexlib/partition/open_executor/preload_executor.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/index/normal/attribute/virtual_attribute_data_appender.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/config/online_config.h"
#include "indexlib/partition/open_executor/open_executor.h"
#include "indexlib/util/memory_control/memory_reserver.h"
#include "indexlib/file_system/indexlib_file_system.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PreloadExecutor);

PreloadExecutor::PreloadExecutor()
{
}

PreloadExecutor::~PreloadExecutor() 
{
}

bool PreloadExecutor::ReserveMem(ExecutorResource& resource, const MemoryReserverPtr& memReserver)
{
    DirectoryPtr rootDirectory = resource.mPartitionDataHolder.Get()->GetRootDirectory();
    size_t incExpandMemSizeInBytes =
        resource.mResourceCalculator->EstimateDiffVersionLockSizeWithoutPatch(
            resource.mSchema, rootDirectory, resource.mIncVersion, resource.mLoadedIncVersion);

    IE_LOG(INFO, "memory : preload need memory [%lu B], current free quota [%ld B]",
           incExpandMemSizeInBytes,
           memReserver->GetFreeQuota());
    if (!memReserver->Reserve(incExpandMemSizeInBytes))
    {
        IE_LOG(WARN, "Preload fail, incExpandMemSize [%luB] over FreeQuota [%ldB]",
               incExpandMemSizeInBytes, memReserver->GetFreeQuota());
        return false;
    }
    return true;
}

bool PreloadExecutor::Execute(ExecutorResource& resource)
{
    misc::ScopeLatencyReporter scopeTime(
        resource.mOnlinePartMetrics.GetpreloadLatencyMetric().get());
    IE_LOG(INFO, "Begin Preload version [%d]", resource.mIncVersion.GetVersionId());
    MemoryReserverPtr memReserver =
        resource.mFileSystem->CreateMemoryReserver("pre_load");

    if (!ReserveMem(resource, memReserver))
    {
        return false;
    }

    bool needDeletionMap = resource.mSchema->GetTableType() != tt_kkv &&
                           resource.mSchema->GetTableType() != tt_kv;
    OnDiskPartitionDataPtr incPartitionData =
        PartitionDataCreator::CreateOnDiskPartitionData(
                resource.mFileSystem, resource.mSchema, resource.mIncVersion,
                "", needDeletionMap, resource.mPluginManager);

    VirtualAttributeDataAppender::PrepareVirtualAttrData(resource.mRtSchema, incPartitionData);

    IndexPartitionOptions options = resource.mOptions;
    options.GetOnlineConfig().disableLoadCustomizedIndex = true;

    mPreloadIncReader.reset(
            new OnlinePartitionReader(options, resource.mSchema,
                    resource.mSearchCache, NULL));
    mPreloadIncReader->Open(incPartitionData);

    IE_LOG(INFO, "End Preload version [%d]", resource.mIncVersion.GetVersionId());
    return true;
}

void PreloadExecutor::Drop(ExecutorResource& resource)
{
    mPreloadIncReader.reset();
}

IE_NAMESPACE_END(partition);

