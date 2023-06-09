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
#include "indexlib/partition/open_executor/dump_container_flush_executor.h"

#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/open_executor/open_executor_util.h"
#include "indexlib/partition/open_executor/reopen_partition_reader_executor.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace std;
using namespace autil;
using namespace indexlib::common;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, DumpContainerFlushExecutor);

DumpContainerFlushExecutor::DumpContainerFlushExecutor(IndexPartition* indexPartition,
                                                       autil::RecursiveThreadMutex* dataLock,
                                                       const string& partitionName, bool isLocked)
    : OpenExecutor(partitionName)
    , mDataLock(dataLock)
    , mIndexPartition(indexPartition)
    , mIsLocked(isLocked)
{
}

DumpContainerFlushExecutor::~DumpContainerFlushExecutor() {}

bool DumpContainerFlushExecutor::Execute(ExecutorResource& resource)
{
    util::ScopeLatencyReporter scopeTime(
        mIsLocked ? resource.mOnlinePartMetrics.GetLockedDumpContainerFlushLatencyMetric().get()
                  : resource.mOnlinePartMetrics.GetUnlockedDumpContainerFlushLatencyMetric().get());
    IE_LOG(INFO, "flush dump container begin");
    DumpSegmentContainerPtr dumpSegmentContainer = resource.mDumpSegmentContainer;
    size_t dumpSize = dumpSegmentContainer->GetUnDumpedSegmentSize();
    for (size_t i = 0; i < dumpSize; ++i) {
        NormalSegmentDumpItemPtr item = dumpSegmentContainer->GetOneValidSegmentItemToDump();
        if (!item) {
            break;
        }
        if (!item->DumpWithMemLimit()) {
            return false;
        }
        HandleDumppedSegment(resource);
    }
    IE_LOG(INFO, "flush dump container end. dump seg cnt[%lu].", dumpSize);
    return true;
}

void DumpContainerFlushExecutor::HandleDumppedSegment(ExecutorResource& resource)
{
    if (resource.mSchema->GetTableType() != tt_kv && resource.mSchema->GetTableType() != tt_kkv) {
        mIndexPartition->RedoOperations();
    } else {
        ReopenNewSegment(resource);
    }
}

void DumpContainerFlushExecutor::ReopenNewSegment(ExecutorResource& resource)
{
    ScopedLock lock(*mDataLock);
    resource.mPartitionDataHolder.Get()->CommitVersion();
    PartitionDataPtr partData = resource.mPartitionDataHolder.Get();
    if (resource.mWriter) {
        partData->CreateNewSegment();
    }
    OpenExecutorUtil::InitReader(resource, partData, PatchLoaderPtr(), mPartitionName);
    PartitionModifierPtr modifier = PartitionModifierCreator::CreateInplaceModifier(resource.mSchema, resource.mReader);
    if (resource.mWriter) {
        resource.mWriter->ReOpenNewSegment(modifier);
    }
    resource.mReader->EnableAccessCountors(resource.mNeedReportTemperatureAccess);
}
}} // namespace indexlib::partition
