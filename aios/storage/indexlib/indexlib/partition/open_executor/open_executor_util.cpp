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
#include "indexlib/partition/open_executor/open_executor_util.h"

#include "indexlib/document/index_locator.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/partition/reader_container.h"

using namespace std;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OpenExecutorUtil);
using indexlib::index_base::PartitionDataPtr;

OpenExecutorUtil::OpenExecutorUtil() {}

OpenExecutorUtil::~OpenExecutorUtil() {}

PartitionModifierPtr OpenExecutorUtil::CreateInplaceModifier(const config::IndexPartitionOptions& options,
                                                             config::IndexPartitionSchemaPtr schema,
                                                             const PartitionDataPtr& partData)
{
    segmentid_t lastValidLinkRtSegId = partData->GetLastValidRtSegmentInLinkDirectory();
    OnlinePartitionReaderPtr reader(
        new partition::OnlinePartitionReader(options, schema, nullptr, nullptr, lastValidLinkRtSegId));
    reader->Open(partData);
    PartitionModifierPtr modifier(PartitionModifierCreator::CreateInplaceModifier(schema, reader));
    return modifier;
}

OnlinePartitionReaderPtr OpenExecutorUtil::GetPatchedReader(ExecutorResource& resource,
                                                            const PartitionDataPtr& partData,
                                                            const PatchLoaderPtr& patchLoader,
                                                            const string& partitionName,
                                                            const OnlinePartitionReader* hintReader)
{
    versionid_t versionId = partData->GetVersion().GetVersionId();
    IE_LOG(INFO, "[%s] Begin open index partition reader (version=[%d])", partitionName.c_str(), versionId);

    segmentid_t lastValidLinkRtSegId = partData->GetLastValidRtSegmentInLinkDirectory();
    OnlinePartitionReaderPtr reader(new OnlinePartitionReader(
        resource.mOptions, resource.mSchema, resource.mSearchCache, &resource.mOnlinePartMetrics, lastValidLinkRtSegId,
        partitionName, resource.mLatestNeedSkipIncTs));

    PartitionDataPtr clonedPartitionData(partData->Clone());

    index::AttributeMetrics* attributeMetrics = resource.mOnlinePartMetrics.GetAttributeMetrics();
    assert(attributeMetrics);
    attributeMetrics->ResetMetricsForNewReader();

    reader->Open(clonedPartitionData, hintReader);
    if (patchLoader) {
        util::ScopeLatencyReporter scopeTime(resource.mOnlinePartMetrics.GetLoadReaderPatchLatencyMetric().get());

        PartitionModifierPtr modifier = PartitionModifierCreator::CreateInplaceModifier(reader->GetSchema(), reader);
        IE_LOG(INFO, "load reader patch begin");
        if (modifier) {
            patchLoader->Load(resource.mLoadedIncVersion, modifier);
        }
        IE_LOG(INFO, "load reader patch end");
    }
    resource.mResourceCalculator->CalculateCurrentIndexSize(resource.mPartitionDataHolder.Get(), resource.mSchema);
    return reader;
}

void OpenExecutorUtil::InitReader(ExecutorResource& resource, const PartitionDataPtr& partData,
                                  const PatchLoaderPtr& patchLoader, const string& partitionName,
                                  const OnlinePartitionReader* hintReader)
{
    IE_LOG(INFO, "[%s] Begin open index partition reader", partitionName.c_str());
    OnlinePartitionReaderPtr reader =
        OpenExecutorUtil::GetPatchedReader(resource, partData, patchLoader, partitionName, hintReader);
    versionid_t versionId = partData->GetVersion().GetVersionId();
    SwitchReader(resource, reader);
    IE_LOG(INFO, "[%s] End open index partition reader (version=[%d])", partitionName.c_str(), versionId);
}

void OpenExecutorUtil::SwitchReader(ExecutorResource& resource, const OnlinePartitionReaderPtr& reader)
{
    {
        autil::ScopedLock lock(resource.mReaderLock);
        resource.mReader = reader;
    }
    resource.mReaderContainer->AddReader(reader);
}

void OpenExecutorUtil::InitWriter(ExecutorResource& resource, const PartitionDataPtr& partData)
{
    if (!resource.mWriter) {
        return;
    }
    PartitionModifierPtr modifier =
        PartitionModifierCreator::CreateInplaceModifier(resource.mReader->GetSchema(), resource.mReader);
    resource.mWriter->Open(resource.mRtSchema, partData, modifier);
}

size_t OpenExecutorUtil::EstimateReopenMemoryUse(ExecutorResource& resource, const std::string& partitionName)
{
    BuildingPartitionDataPtr snapshotBuildingPartData =
        DYNAMIC_POINTER_CAST(BuildingPartitionData, resource.mSnapshotPartitionData);
    PartitionDataPtr partData = resource.mSnapshotPartitionData;
    file_system::DirectoryPtr rootDirectory = partData->GetRootDirectory();
    size_t diffVersionLockSizeWithoutPatch = resource.mResourceCalculator->EstimateDiffVersionLockSizeWithoutPatch(
        resource.mSchema, rootDirectory, resource.mIncVersion, resource.mLoadedIncVersion);

    index_base::OnlineJoinPolicy joinPolicy(resource.mIncVersion, resource.mSchema->GetTableType(),
                                            resource.mSrcSignature);
    int64_t reclaimTimestamp = joinPolicy.GetReclaimRtTimestamp();

    size_t redoOperationExpandSize =
        resource.mResourceCalculator->EstimateRedoOperationExpandSize(resource.mSchema, partData, reclaimTimestamp);
    util::CounterMapPtr counterMap;
    BuildingPartitionParam param(resource.mOptions, resource.mRtSchema, resource.mPartitionMemController,
                                 snapshotBuildingPartData->GetDumpSegmentContainer(), counterMap,
                                 resource.mPluginManager, resource.mOnlinePartMetrics.GetMetricProvider(),
                                 resource.mSrcSignature);
    PartitionDataPtr newPartitionData = PartitionDataCreator::CreateBranchBuildingPartitionData(
        param, partData->GetSegmentDirectory(), resource.mIncVersion, index_base::InMemorySegmentPtr(), true);
    resource.mBranchPartitionDataHolder.Reset(newPartitionData);

    PatchLoaderPtr patchLoader = CreatePatchLoader(resource, newPartitionData, partitionName, false);
    size_t patchLoadExpandSize = patchLoader ? patchLoader->CalculatePatchLoadExpandSize() : 0u;
    IE_LOG(INFO,
           "estimate reopen memory use, version from [%d] to [%d]: "
           "diffVersionLockSizeWithoutPatch [%lu], "
           "redoOperationExpandSize [%lu], patchLoadExpandSize [%lu]",
           resource.mLoadedIncVersion.GetVersionId(), resource.mIncVersion.GetVersionId(),
           diffVersionLockSizeWithoutPatch, redoOperationExpandSize, patchLoadExpandSize);
    return diffVersionLockSizeWithoutPatch + redoOperationExpandSize + patchLoadExpandSize;
}

PatchLoaderPtr OpenExecutorUtil::CreatePatchLoader(const ExecutorResource& resource,
                                                   const PartitionDataPtr& partitionData, const string& partitionName,
                                                   bool ignorePatchToOldIncSegment)
{
    bool loadPatchForOpen = false;
    if (resource.mIndexPhase == IndexPartition::OPENING || resource.mIndexPhase == IndexPartition::FORCE_OPEN ||
        resource.mIndexPhase == IndexPartition::FORCE_REOPEN) {
        loadPatchForOpen = true;
    }
    PatchLoaderPtr patchLoader(new PatchLoader(resource.mRtSchema, resource.mOptions.GetOnlineConfig()));
    segmentid_t startLoadSegment = 0;
    if (resource.mLoadedIncVersion != index_base::Version(INVALID_VERSIONID)) {
        startLoadSegment = resource.mLoadedIncVersion.GetLastSegment() + 1;
    }
    string locatorStr = partitionData->GetLastLocator();
    index_base::Version onDiskIncVersion = partitionData->GetOnDiskVersion();

    bool isIncConsistentWithRt = (resource.mLoadedIncVersion != index_base::Version(INVALID_VERSIONID)) &&
                                 resource.mOptions.GetOnlineConfig().isIncConsistentWithRealtime;

    document::IndexLocator indexLocator;
    if (!indexLocator.fromString(locatorStr)) {
        IE_LOG(INFO, "from locator string [%s] failed", locatorStr.c_str());
        isIncConsistentWithRt = false;
    } else {
        bool fromInc = false;
        index_base::OnlineJoinPolicy joinPolicy(onDiskIncVersion, resource.mSchema->GetTableType(),
                                                resource.mSrcSignature);
        joinPolicy.GetRtSeekTimestamp(indexLocator, fromInc);
        IE_LOG(INFO, "ondisk inc version [%s], index locator [%s]", ToJsonString(onDiskIncVersion).c_str(),
               indexLocator.toDebugString().c_str());
        if (fromInc) {
            isIncConsistentWithRt = false;
        }
    }
    bool ignore = isIncConsistentWithRt;
    if (ignorePatchToOldIncSegment) {
        ignore = true;
    }
    IE_LOG(INFO, "[%s] load reader patch with isIncConsistentWithRt[%d], start segment [%d]", partitionName.c_str(),
           isIncConsistentWithRt, startLoadSegment);
    patchLoader->Init(partitionData, ignore, resource.mLoadedIncVersion, startLoadSegment, loadPatchForOpen);
    return patchLoader;
}
}} // namespace indexlib::partition
