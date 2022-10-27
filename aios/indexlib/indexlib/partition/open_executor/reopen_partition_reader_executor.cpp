#include "indexlib/partition/open_executor/reopen_partition_reader_executor.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/index/normal/attribute/virtual_attribute_data_appender.h"
#include "indexlib/misc/metric_provider.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/index/online_join_policy.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"
#include "indexlib/util/memory_control/memory_reserver.h"
#include "indexlib/common/index_locator.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, ReopenPartitionReaderExecutor);

#define IE_LOG_PREFIX mPartitionName.c_str()

ReopenPartitionReaderExecutor::~ReopenPartitionReaderExecutor() 
{
}

bool ReopenPartitionReaderExecutor::LoadReaderPatch(const OnlinePartitionReaderPtr& reader,
        const PatchLoaderPtr& patchLoader, ExecutorResource& resource)
{
    misc::ScopeLatencyReporter scopeTime(
        resource.mOnlinePartMetrics.GetLoadReaderPatchLatencyMetric().get());
    IE_LOG(INFO, "load reader patch begin");

    PartitionModifierPtr modifier = CreateReaderModifier(reader);
    if (!modifier || !patchLoader)
    {
        return true;
    }
    patchLoader->Load(resource.mLoadedIncVersion, modifier);
    IE_LOG(INFO, "load reader patch end");
    return true;
}

void ReopenPartitionReaderExecutor::SwitchReader(
        ExecutorResource& resource, const OnlinePartitionReaderPtr& reader)
{
    {
        autil::ScopedLock lock(resource.mReaderLock);
        resource.mReader = reader;
    }
    resource.mReaderContainer->AddReader(reader);
}

PatchLoaderPtr ReopenPartitionReaderExecutor::CreatePatchLoader(
        const ExecutorResource& resource, const PartitionDataPtr& partitionData,
        const string& partitionName)
{
    bool loadPatchForOpen = false;
    if (resource.mIndexPhase == IndexPartition::OPENING
        || resource.mIndexPhase == IndexPartition::FORCE_OPEN
        || resource.mIndexPhase == IndexPartition::FORCE_REOPEN)
    {
        loadPatchForOpen = true;
    }
    PatchLoaderPtr patchLoader(
        new PatchLoader(resource.mRtSchema, resource.mOptions.GetOnlineConfig()));
    segmentid_t startLoadSegment = 0;
    if (resource.mLoadedIncVersion != INVALID_VERSION)
    {
        startLoadSegment = resource.mLoadedIncVersion.GetLastSegment() + 1;
    }
    string locatorStr = partitionData->GetLastLocator();    
    index_base::Version onDiskIncVersion = partitionData->GetOnDiskVersion();

    bool isIncConsistentWithRt = (resource.mLoadedIncVersion != INVALID_VERSION)
        && resource.mOptions.GetOnlineConfig().isIncConsistentWithRealtime; 

    IndexLocator indexLocator;
    if (!indexLocator.fromString(locatorStr))
    {
        isIncConsistentWithRt = false;
    }
    else
    {
        bool fromInc = false;
        OnlineJoinPolicy joinPolicy(onDiskIncVersion, resource.mSchema->GetTableType());
        joinPolicy.GetRtSeekTimestamp(indexLocator, fromInc);
        if (fromInc)
        {
            isIncConsistentWithRt =  false;
        }
    }    
    IE_LOG(INFO, "[%s] load reader patch with isIncConsistentWithRt[%d]",
           partitionName.c_str(), isIncConsistentWithRt);
    patchLoader->Init(partitionData, isIncConsistentWithRt,
                      resource.mLoadedIncVersion, startLoadSegment, loadPatchForOpen);
    return patchLoader;
}

size_t ReopenPartitionReaderExecutor::EstimateReopenMemoryUse(
        const ExecutorResource& resource, const std::string& partitionName)
{
    PartitionDataPtr partData = resource.mPartitionDataHolder.Get();
    DirectoryPtr rootDirectory = partData->GetRootDirectory();
    size_t diffVersionLockSizeWithoutPatch =
        resource.mResourceCalculator->EstimateDiffVersionLockSizeWithoutPatch(
            resource.mSchema, rootDirectory,
            resource.mIncVersion, resource.mLoadedIncVersion);
    
    OnlineJoinPolicy joinPolicy(
        resource.mIncVersion, resource.mSchema->GetTableType());
    int64_t reclaimTimestamp = joinPolicy.GetReclaimRtTimestamp();
    
    size_t redoOperationExpandSize =
        resource.mResourceCalculator->EstimateRedoOperationExpandSize(
            resource.mSchema, partData, reclaimTimestamp);
    PartitionDataPtr newPartitionData =
        PartitionDataCreator::CreateBuildingPartitionData(
            resource.mFileSystem, resource.mRtSchema, resource.mOptions,
            resource.mPartitionMemController, resource.mDumpSegmentContainer,
            resource.mIncVersion, resource.mOnlinePartMetrics.GetMetricProvider(),
            "", InMemorySegmentPtr(), util::CounterMapPtr(), resource.mPluginManager);

    PatchLoaderPtr patchLoader = CreatePatchLoader(resource, newPartitionData, partitionName);
    size_t patchLoadExpandSize = patchLoader ? patchLoader->CalculatePatchLoadExpandSize()
                                             : 0u;
    IE_LOG(INFO, "estimate reopen memory use, version from [%d] to [%d]: "
           "diffVersionLockSizeWithoutPatch [%lu], "
           "redoOperationExpandSize [%lu], patchLoadExpandSize [%lu]",
           resource.mLoadedIncVersion.GetVersionId(), resource.mIncVersion.GetVersionId(), 
           diffVersionLockSizeWithoutPatch, redoOperationExpandSize, patchLoadExpandSize);
    return diffVersionLockSizeWithoutPatch + redoOperationExpandSize + patchLoadExpandSize;
}

void ReopenPartitionReaderExecutor::InitReader(ExecutorResource& resource,
        const PartitionDataPtr& partData, const PatchLoaderPtr& patchLoader,
        const string& partitionName)
{
    //assert(partData->GetInMemorySegment());
    versionid_t versionId = partData->GetVersion().GetVersionId();
    IE_LOG(INFO, "[%s] Begin open index partition reader (version=[%d])",
           partitionName.c_str(), versionId);

    segmentid_t lastValidLinkRtSegId =
        partData->GetLastValidRtSegmentInLinkDirectory();
    OnlinePartitionReaderPtr reader(new OnlinePartitionReader(
                    resource.mOptions, resource.mSchema, resource.mSearchCache,
                    &resource.mOnlinePartMetrics, lastValidLinkRtSegId,
                    partitionName, resource.mLatestNeedSkipIncTs));

    PartitionDataPtr clonedPartitionData(partData->Clone());

    AttributeMetrics* attributeMetrics = resource.mOnlinePartMetrics.GetAttributeMetrics();
    assert(attributeMetrics);
    attributeMetrics->ResetMetricsForNewReader();

    reader->Open(clonedPartitionData);
    if (patchLoader)
    {
        LoadReaderPatch(reader, patchLoader, resource);
    }
    resource.mResourceCalculator->CalculateCurrentIndexSize(
            resource.mPartitionDataHolder.Get(), resource.mSchema);
    SwitchReader(resource, reader);
    IE_LOG(INFO, "[%s] End open index partition reader (version=[%d])",
           partitionName.c_str(), versionId);
}

PartitionModifierPtr ReopenPartitionReaderExecutor::CreateReaderModifier(
        const IndexPartitionReaderPtr& reader)
{
    assert(reader);
    PartitionModifierPtr modifier = 
        PartitionModifierCreator::CreateInplaceModifier(
                reader->GetSchema(), reader);
    return modifier;
}

void ReopenPartitionReaderExecutor::InitWriter(ExecutorResource& resource,
        const PartitionDataPtr& partData)
{
    if (!resource.mWriter)
    {
        return;
    }
    PartitionModifierPtr modifier =
        CreateReaderModifier(resource.mReader);
    resource.mWriter->Open(resource.mRtSchema, partData, modifier);
}

bool ReopenPartitionReaderExecutor::CanLoadReader(ExecutorResource& resource,
        const PatchLoaderPtr& patchLoader,
        const MemoryReserverPtr& memReserver)
{
    size_t loadReaderMemUse = 0;
    if (!mHasPreload)
    {
        DirectoryPtr rootDirectory = resource.mPartitionDataHolder.Get()->GetRootDirectory();
        loadReaderMemUse =
            resource.mResourceCalculator->EstimateDiffVersionLockSizeWithoutPatch(
                    resource.mSchema, rootDirectory,
                    resource.mIncVersion, resource.mLoadedIncVersion,
                    util::MultiCounterPtr(), false);
    }

    size_t patchLoadExpandSize = 0;
    if (patchLoader)
    {
        patchLoadExpandSize = patchLoader->CalculatePatchLoadExpandSize();
        loadReaderMemUse += patchLoadExpandSize;
    }
    
    if (!memReserver->Reserve(loadReaderMemUse))
    {
        IE_PREFIX_LOG(WARN, "load reader fail, incExpandMemSize [%lu] over FreeQuota [%ld]"
                      ", patchLoadExpandSize [%ld], version [%d to %d]", 
                      loadReaderMemUse, memReserver->GetFreeQuota(), patchLoadExpandSize,
                      resource.mLoadedIncVersion.GetVersionId(),
                      resource.mIncVersion.GetVersionId());
        return false;
    }
    IE_PREFIX_LOG(INFO, "can load reader, incExpandMemSize [%lu], left FreeQuota[%ld]"
                  ", patchLoadExpandSize[%ld], version [%d to %d]", 
                  loadReaderMemUse, memReserver->GetFreeQuota(), patchLoadExpandSize,
                  resource.mLoadedIncVersion.GetVersionId(),
                  resource.mIncVersion.GetVersionId());
    return true;
}
bool ReopenPartitionReaderExecutor::Execute(ExecutorResource& resource)
{
    misc::ScopeLatencyReporter scopeTime(
            resource.mOnlinePartMetrics.GetReopenFullPartitionLatencyMetric().get());
    IE_PREFIX_LOG(INFO, "reopen partition reader begin");

    InMemorySegmentPtr orgInMemSegment;
    if (resource.mNeedInheritInMemorySegment)
    {
        orgInMemSegment = resource.mPartitionDataHolder.Get()->GetInMemorySegment();
    }

    PartitionDataPtr newPartitionData =
        PartitionDataCreator::CreateBuildingPartitionData(
                resource.mFileSystem, resource.mRtSchema, resource.mOptions,
                resource.mPartitionMemController, resource.mDumpSegmentContainer,
                resource.mIncVersion, resource.mOnlinePartMetrics.GetMetricProvider(),
                "", orgInMemSegment, resource.mCounterMap, resource.mPluginManager);

    PatchLoaderPtr patchLoader = CreatePatchLoader(resource, newPartitionData, mPartitionName);
    const MemoryReserverPtr memReserver =
        resource.mFileSystem->CreateMemoryReserver("reopen_partition_reader_executor");
    if (!CanLoadReader(resource, patchLoader, memReserver))
    {
        return false;
    }

    VirtualAttributeDataAppender::PrepareVirtualAttrData(resource.mRtSchema, newPartitionData);
    if (resource.mWriter)
    {
        newPartitionData->CreateNewSegment();
    }
    InitReader(resource, newPartitionData, patchLoader, mPartitionName);
    InitWriter(resource, newPartitionData);

    resource.mPartitionDataHolder.Reset(newPartitionData);
    resource.mReader->EnableAccessCountors();

    // after the new reader is switched, mLoadedIncVersion can be updated
    resource.mLoadedIncVersion = resource.mIncVersion;
    IE_PREFIX_LOG(INFO, "reopen partition reader end");
    return true;
}

void ReopenPartitionReaderExecutor::Drop(ExecutorResource& resource)
{
}

#undef IE_LOG_PREFIX

IE_NAMESPACE_END(partition);

