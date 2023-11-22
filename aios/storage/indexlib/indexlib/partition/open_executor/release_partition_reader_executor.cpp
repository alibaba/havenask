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
#include "indexlib/partition/open_executor/release_partition_reader_executor.h"

#include "autil/Lock.h"
#include "indexlib/common/executor_scheduler.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/reader_container.h"

using namespace std;
using namespace autil;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, ReleasePartitionReaderExecutor);

IndexPartitionReaderPtr ReleasePartitionReaderExecutor::CreateRealtimeReader(ExecutorResource& resource)
{
    // TODO: make code readable
    // partition data not use on disk segment
    Version version = resource.mPartitionDataHolder.Get()->GetOnDiskVersion();
    version.Clear();
    // building partition data can auto scan rt partition and join partition
    util::CounterMapPtr counterMap;
    util::MetricProviderPtr metricProvider;
    BuildingPartitionParam param(resource.mOptions, resource.mRtSchema, resource.mPartitionMemController,
                                 resource.mDumpSegmentContainer, counterMap, resource.mPluginManager, metricProvider,
                                 resource.mSrcSignature);
    BuildingPartitionDataPtr realtimePartitionData = PartitionDataCreator::CreateBuildingPartitionData(
        param, resource.mFileSystem, version, "", InMemorySegmentPtr());

    segmentid_t lastValidLinkRtSegId = realtimePartitionData->GetLastValidRtSegmentInLinkDirectory();
    OnlinePartitionReaderPtr reader(
        new partition::OnlinePartitionReader(resource.mOptions, resource.mSchema, resource.mSearchCache,
                                             &resource.mOnlinePartMetrics, lastValidLinkRtSegId));
    reader->Open(realtimePartitionData);
    return reader;
}

void ReleasePartitionReaderExecutor::CleanResource(ExecutorResource& resource)
{
    IE_LOG(DEBUG, "resource clean begin");
    ScopedLock lock(*resource.mCleanerLock);
    if (resource.mResourceCleaner) {
        resource.mResourceCleaner->Execute();
    }
    IE_LOG(DEBUG, "resource clean end");
}

bool ReleasePartitionReaderExecutor::Execute(ExecutorResource& resource)
{
    IE_LOG(INFO, "release partition reader executor begin");
    mLastLoadedIncVersion = resource.mLoadedIncVersion;
    resource.mReader.reset();
    if (resource.mWriter) {
        resource.mWriter->Reset();
    }
    IndexPartitionReaderPtr reader = CreateRealtimeReader(resource);
    InMemorySegmentPtr orgInMemSegment;
    if (resource.mNeedInheritInMemorySegment) {
        orgInMemSegment = resource.mPartitionDataHolder.Get()->GetInMemorySegment();
    }
    resource.mPartitionDataHolder.Reset();
    CleanResource(resource);
    util::CounterMapPtr counterMap;
    BuildingPartitionParam param(resource.mOptions, resource.mRtSchema, resource.mPartitionMemController,
                                 resource.mDumpSegmentContainer, counterMap, resource.mPluginManager,
                                 resource.mOnlinePartMetrics.GetMetricProvider(), resource.mSrcSignature);
    resource.mPartitionDataHolder.Reset(PartitionDataCreator::CreateBuildingPartitionData(
        param, resource.mFileSystem, resource.mLoadedIncVersion, "", orgInMemSegment));
    reader.reset();
    assert(resource.mReaderContainer->Size() == 0);
    resource.mLoadedIncVersion = index_base::Version(INVALID_VERSIONID);
    IE_LOG(INFO, "release partition reader executor end");
    return true;
}
}} // namespace indexlib::partition
