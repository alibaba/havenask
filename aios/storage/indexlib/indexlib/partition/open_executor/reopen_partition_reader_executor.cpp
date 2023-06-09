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
#include "indexlib/partition/open_executor/reopen_partition_reader_executor.h"

#include "autil/TimeUtility.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index/normal/attribute/virtual_attribute_data_appender.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/open_executor/open_executor_util.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/MemoryReserver.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, ReopenPartitionReaderExecutor);

#define IE_LOG_PREFIX mPartitionName.c_str()

ReopenPartitionReaderExecutor::~ReopenPartitionReaderExecutor() {}

bool ReopenPartitionReaderExecutor::CanLoadReader(ExecutorResource& resource, const PatchLoaderPtr& patchLoader,
                                                  const MemoryReserverPtr& memReserver)
{
    size_t loadReaderMemUse = 0;
    if (!mHasPreload) {
        DirectoryPtr rootDirectory = resource.mPartitionDataHolder.Get()->GetRootDirectory();
        loadReaderMemUse = resource.mResourceCalculator->EstimateDiffVersionLockSizeWithoutPatch(
            resource.mSchema, rootDirectory, resource.mIncVersion, resource.mLoadedIncVersion, util::MultiCounterPtr(),
            false);
    }

    size_t patchLoadExpandSize = 0;
    if (patchLoader) {
        patchLoadExpandSize = patchLoader->CalculatePatchLoadExpandSize();
        loadReaderMemUse += patchLoadExpandSize;
    }

    if (!memReserver->Reserve(loadReaderMemUse)) {
        IE_PREFIX_LOG(WARN,
                      "load reader fail, incExpandMemSize [%lu] over FreeQuota [%ld]"
                      ", patchLoadExpandSize [%ld], version [%d to %d]",
                      loadReaderMemUse, memReserver->GetFreeQuota(), patchLoadExpandSize,
                      resource.mLoadedIncVersion.GetVersionId(), resource.mIncVersion.GetVersionId());
        return false;
    }
    IE_PREFIX_LOG(INFO,
                  "can load reader, incExpandMemSize [%lu], left FreeQuota[%ld]"
                  ", patchLoadExpandSize[%ld], version [%d to %d]",
                  loadReaderMemUse, memReserver->GetFreeQuota(), patchLoadExpandSize,
                  resource.mLoadedIncVersion.GetVersionId(), resource.mIncVersion.GetVersionId());
    return true;
}
bool ReopenPartitionReaderExecutor::Execute(ExecutorResource& resource)
{
    util::ScopeLatencyReporter scopeTime(resource.mOnlinePartMetrics.GetReopenFullPartitionLatencyMetric().get());
    IE_PREFIX_LOG(INFO, "reopen partition reader begin");

    InMemorySegmentPtr orgInMemSegment;
    if (resource.mNeedInheritInMemorySegment) {
        orgInMemSegment = resource.mPartitionDataHolder.Get()->GetInMemorySegment();
    }

    BuildingPartitionParam param(resource.mOptions, resource.mRtSchema, resource.mPartitionMemController,
                                 resource.mDumpSegmentContainer, resource.mCounterMap, resource.mPluginManager,
                                 resource.mOnlinePartMetrics.GetMetricProvider(), resource.mSrcSignature);
    PartitionDataPtr newPartitionData = PartitionDataCreator::CreateBuildingPartitionData(
        param, resource.mFileSystem, resource.mIncVersion, "", orgInMemSegment);

    PatchLoaderPtr patchLoader;
    if (resource.mLoadedIncVersion != resource.mIncVersion) {
        patchLoader = OpenExecutorUtil::CreatePatchLoader(resource, newPartitionData, mPartitionName, false);
    }
    const MemoryReserverPtr memReserver =
        resource.mFileSystem->CreateMemoryReserver("reopen_partition_reader_executor");
    if (!CanLoadReader(resource, patchLoader, memReserver)) {
        return false;
    }

    VirtualAttributeDataAppender::PrepareVirtualAttrData(resource.mRtSchema, newPartitionData);
    if (resource.mWriter) {
        newPartitionData->CreateNewSegment();
    }
    OpenExecutorUtil::InitReader(resource, newPartitionData, patchLoader, mPartitionName,
                                 resource.mPreloadReader.get());
    OpenExecutorUtil::InitWriter(resource, newPartitionData);

    resource.mPartitionDataHolder.Reset(newPartitionData);
    resource.mReader->EnableAccessCountors(resource.mNeedReportTemperatureAccess);

    // after the new reader is switched, mLoadedIncVersion can be updated
    resource.mLoadedIncVersion = resource.mIncVersion;
    IE_PREFIX_LOG(INFO, "reopen partition reader end, used[%.3f]s", scopeTime.GetTimer().done_sec());
    return true;
}

void ReopenPartitionReaderExecutor::Drop(ExecutorResource& resource) {}

#undef IE_LOG_PREFIX
}} // namespace indexlib::partition
