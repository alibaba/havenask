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
#include "indexlib/partition/open_executor/open_executor_chain_creator.h"

#include "indexlib/document/index_locator.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/open_executor/dump_container_flush_executor.h"
#include "indexlib/partition/open_executor/dump_segment_executor.h"
#include "indexlib/partition/open_executor/generate_join_segment_executor.h"
#include "indexlib/partition/open_executor/lock_executor.h"
#include "indexlib/partition/open_executor/prejoin_executor.h"
#include "indexlib/partition/open_executor/preload_executor.h"
#include "indexlib/partition/open_executor/prepatch_executor.h"
#include "indexlib/partition/open_executor/realtime_index_recover_executor.h"
#include "indexlib/partition/open_executor/reclaim_rt_index_executor.h"
#include "indexlib/partition/open_executor/reclaim_rt_segments_executor.h"
#include "indexlib/partition/open_executor/redo_and_lock_executor.h"
#include "indexlib/partition/open_executor/release_partition_reader_executor.h"
#include "indexlib/partition/open_executor/reopen_partition_reader_executor.h"
#include "indexlib/partition/open_executor/scoped_lock_executor.h"
#include "indexlib/partition/open_executor/switch_branch_executor.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::plugin;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OpenExecutorChainCreator);

JoinSegmentWriterPtr OpenExecutorChainCreator::CreateJoinSegmentWriter(
    config::IndexPartitionSchemaPtr schema, file_system::IFileSystemPtr fileSystem,
    util::PartitionMemoryQuotaControllerPtr partitionMemController,
    index_base::PartitionDataHolder& partitionDataHolder, index_base::Version loadedIncVersion,
    const Version& onDiskVersion, config::IndexPartitionOptions& options, OnlinePartitionMetricsPtr onlinePartMetrics,
    bool isForceReopen) const
{
    util::ScopeLatencyReporter scopeTime(onlinePartMetrics->GetcreateJoinSegmentWriterLatencyMetric().get());

    bool enableRedoSpeedup = options.GetOnlineConfig().IsRedoSpeedupEnabled() && !isForceReopen;

    IE_LOG(INFO, "enableRedoSpeedup flag = [%d] on version[%d] with isForceReopen = [%d]", enableRedoSpeedup,
           onDiskVersion.GetVersionId(), isForceReopen);

    IndexPartitionOptions copiedOptions = options;
    copiedOptions.GetOnlineConfig().SetEnableRedoSpeedup(enableRedoSpeedup);

    JoinSegmentWriterPtr joinWriter(new JoinSegmentWriter(schema, copiedOptions, fileSystem, partitionMemController));
    joinWriter->Init(partitionDataHolder.Get(), onDiskVersion, loadedIncVersion);
    return joinWriter;
}

bool OpenExecutorChainCreator::CanOptimizedReopen(config::IndexPartitionOptions& options,
                                                  config::IndexPartitionSchemaPtr schema,
                                                  index_base::Version& onDiskVersion) const
{
    string locatorStr = mPartition->GetLastLocator();
    document::IndexLocator locator;
    if (!locator.fromString(locatorStr)) {
        return false;
    }
    bool fromInc = false;
    OnlineJoinPolicy joinPolicy(onDiskVersion, schema->GetTableType(), mPartition->GetSrcSignature());
    joinPolicy.GetRtSeekTimestamp(locator, fromInc);
    IE_LOG(INFO, "rt locator [%ld], onDiskVersion [%s], freom inc [%d]", locator.getOffset(),
           autil::legacy::ToJsonString(onDiskVersion).c_str(), fromInc);
    if (fromInc) {
        return false;
    }
    if (options.GetOnlineConfig().IsOptimizedReopen()) {
        return true;
    }
    return false;
}

OpenExecutorChainPtr OpenExecutorChainCreator::CreateReopenExecutorChain(
    config::IndexPartitionSchemaPtr schema, file_system::IFileSystemPtr fileSystem,
    util::PartitionMemoryQuotaControllerPtr partitionMemController,
    index_base::PartitionDataHolder& partitionDataHolder, index_base::Version loadedIncVersion,
    index_base::Version& onDiskVersion, ScopedLockPtr& scopedLock, autil::RecursiveThreadMutex& dataLock,
    autil::ThreadMutex& dumpLock, config::IndexPartitionOptions& options, OnlinePartitionMetricsPtr onlinePartMetrics,
    bool enableAsyncDump, int64_t redoTime)
{
    OpenExecutorChainPtr chain = createOpenExecutorChain();
    if (schema->GetTableType() != tt_kkv && schema->GetTableType() != tt_kv) {
        bool needOptimize = CanOptimizedReopen(options, schema, onDiskVersion);
        if (!needOptimize) {
            JoinSegmentWriterPtr joinSegWriter =
                CreateJoinSegmentWriter(schema, fileSystem, partitionMemController, partitionDataHolder,
                                        loadedIncVersion, onDiskVersion, options, onlinePartMetrics, false);
            // release dataLock for rt build when preload
            chain->PushBack(CreateScopedUnlockExecutor(scopedLock, dataLock));
            chain->PushBack(CreatePreloadExecutor());
            chain->PushBack(CreatePreJoinExecutor(joinSegWriter));
            // push current building segment to dump container
            chain->PushBack(CreateScopedLockExecutor(scopedLock, dataLock));
            chain->PushBack(CreateDumpSegmentExecutor(enableAsyncDump));
            if (enableAsyncDump) {
                chain->PushBack(CreateScopedUnlockExecutor(scopedLock, dataLock));
                chain->PushBack(
                    OpenExecutorPtr(new DumpContainerFlushExecutor(mPartition, &dataLock, mPartitionName, false)));
                chain->PushBack(CreateScopedLockExecutor(scopedLock, dataLock));
                chain->PushBack(CreateDumpSegmentExecutor());
                // push left building doc to dump container
                chain->PushBack(
                    OpenExecutorPtr(new DumpContainerFlushExecutor(mPartition, &dataLock, mPartitionName, true)));
            }
            chain->PushBack(CreateReclaimRtIndexExecutor(false));
            chain->PushBack(CreateGenerateJoinSegmentExecutor(joinSegWriter));
            chain->PushBack(CreateReopenPartitionReaderExecutor(true));
        } else {
            chain->PushBack(CreateScopedUnlockExecutor(scopedLock, dataLock));
            chain->PushBack(
                OpenExecutorPtr(new DumpContainerFlushExecutor(mPartition, &dataLock, mPartitionName, false)));
            // chain->PushBack(CreateLockExecutor(&dumpLock));
            chain->PushBack(CreatePreloadExecutor());
            chain->PushBack(CreatePrepatchExecutor(&dataLock));
            chain->PushBack(CreateReclaimRtIndexExecutor(true));
            chain->PushBack(CreateRedoAndLockExecutor(&dataLock, redoTime));
            chain->PushBack(CreateSwitchBranchExecutor());
            // chain->PushBack(CreateUnLockExecutor(&dumpLock));
            chain->PushBack(CreateUnLockExecutor(&dataLock));
            chain->PushBack(CreateScopedLockExecutor(scopedLock, dataLock));
        }
    } else {
        // release dataLock for rt build when preload
        chain->PushBack(CreateScopedUnlockExecutor(scopedLock, dataLock));
        chain->PushBack(CreatePreloadExecutor());
        chain->PushBack(OpenExecutorPtr(new DumpContainerFlushExecutor(mPartition, &dataLock, mPartitionName, false)));

        chain->PushBack(CreateScopedLockExecutor(scopedLock, dataLock));
        chain->PushBack(OpenExecutorPtr(new DumpContainerFlushExecutor(mPartition, &dataLock, mPartitionName, true)));
        chain->PushBack(CreateReclaimRtSegmentsExecutor(true));
        chain->PushBack(CreateReopenPartitionReaderExecutor(true));
    }
    return chain;
}

OpenExecutorChainPtr OpenExecutorChainCreator::CreateReopenPartitionReaderChain(bool hasPreload)
{
    OpenExecutorChainPtr chain(new OpenExecutorChain);
    chain->PushBack(CreateReopenPartitionReaderExecutor(hasPreload));
    return chain;
}

OpenExecutorChainPtr OpenExecutorChainCreator::CreateForceReopenExecutorChain(
    config::IndexPartitionSchemaPtr schema, file_system::IFileSystemPtr fileSystem,
    util::PartitionMemoryQuotaControllerPtr partitionMemController,
    index_base::PartitionDataHolder& partitionDataHolder, index_base::Version loadedIncVersion,
    index_base::Version& onDiskVersion, autil::RecursiveThreadMutex& dataLock, config::IndexPartitionOptions& options,
    OnlinePartitionMetricsPtr onlinePartMetrics, bool enableAsyncDump)
{
    OpenExecutorChainPtr chain(new OpenExecutorChain);
    if (schema->GetTableType() != tt_kkv && schema->GetTableType() != tt_kv) {
        JoinSegmentWriterPtr joinSegWriter =
            CreateJoinSegmentWriter(schema, fileSystem, partitionMemController, partitionDataHolder, loadedIncVersion,
                                    onDiskVersion, options, onlinePartMetrics, true);

        chain->PushBack(OpenExecutorPtr(new DumpSegmentExecutor(mPartitionName, enableAsyncDump)));
        if (enableAsyncDump) {
            chain->PushBack(
                OpenExecutorPtr(new DumpContainerFlushExecutor(mPartition, &dataLock, mPartitionName, true)));
        }
        chain->PushBack(CreateReclaimRtIndexExecutor());
        chain->PushBack(CreateGenerateJoinSegmentExecutor(joinSegWriter));
        if (loadedIncVersion != index_base::Version(INVALID_VERSION)) {
            chain->PushBack(CreateReleaseReaderExecutor());
        }
        chain->PushBack(CreateReopenPartitionReaderExecutor(false));
    } else {
        chain->PushBack(OpenExecutorPtr(new DumpContainerFlushExecutor(mPartition, &dataLock, mPartitionName, true)));
        chain->PushBack(CreateReclaimRtSegmentsExecutor(true));
        if (loadedIncVersion != index_base::Version(INVALID_VERSION)) {
            chain->PushBack(CreateReleaseReaderExecutor());
        }
        chain->PushBack(CreateReopenPartitionReaderExecutor(false));
    }
    return chain;
}

OpenExecutorPtr OpenExecutorChainCreator::CreatePreloadExecutor() { return OpenExecutorPtr(new PreloadExecutor()); }
OpenExecutorPtr OpenExecutorChainCreator::CreatePrepatchExecutor(autil::RecursiveThreadMutex* dataLock)
{
    return OpenExecutorPtr(new PrepatchExecutor(dataLock));
}
OpenExecutorPtr OpenExecutorChainCreator::CreatePreJoinExecutor(const JoinSegmentWriterPtr& joinSegWriter)
{
    return OpenExecutorPtr(new PrejoinExecutor(joinSegWriter));
}

OpenExecutorPtr OpenExecutorChainCreator::CreateScopedLockExecutor(ScopedLockPtr& scopedLock,
                                                                   autil::ThreadMutex& dataLock)
{
    return OpenExecutorPtr(new ScopedLockExecutor(scopedLock, dataLock));
}

OpenExecutorPtr OpenExecutorChainCreator::CreateScopedUnlockExecutor(ScopedLockPtr& scopedLock,
                                                                     autil::ThreadMutex& dataLock)
{
    return OpenExecutorPtr(new ScopedUnlockExecutor(scopedLock, dataLock));
}

OpenExecutorPtr OpenExecutorChainCreator::CreateLockExecutor(autil::ThreadMutex* lock)
{
    return OpenExecutorPtr(new LockExecutor(lock));
}

OpenExecutorPtr OpenExecutorChainCreator::CreateUnLockExecutor(autil::ThreadMutex* lock)
{
    return OpenExecutorPtr(new UnLockExecutor(lock));
}

OpenExecutorPtr OpenExecutorChainCreator::CreateRedoAndLockExecutor(autil::ThreadMutex* lock, int64_t redoTime)
{
    return OpenExecutorPtr(new RedoAndLockExecutor(lock, redoTime));
}

OpenExecutorPtr OpenExecutorChainCreator::CreateSwitchBranchExecutor()
{
    return OpenExecutorPtr(new SwitchBranchExecutor(mPartitionName));
}

OpenExecutorPtr OpenExecutorChainCreator::CreateDumpSegmentExecutor(bool reInitReaderAndWriter)
{
    return OpenExecutorPtr(new DumpSegmentExecutor(mPartitionName, reInitReaderAndWriter));
}
OpenExecutorPtr OpenExecutorChainCreator::CreateReclaimRtIndexExecutor(bool isInplaceReclaim)
{
    return OpenExecutorPtr(new ReclaimRtIndexExecutor(isInplaceReclaim));
}
OpenExecutorPtr OpenExecutorChainCreator::CreateReclaimRtSegmentsExecutor(bool reclaimBuildingSegment)
{
    return OpenExecutorPtr(new ReclaimRtSegmentsExecutor(reclaimBuildingSegment));
}
OpenExecutorPtr OpenExecutorChainCreator::CreateGenerateJoinSegmentExecutor(const JoinSegmentWriterPtr& joinSegWriter)
{
    return OpenExecutorPtr(new GenerateJoinSegmentExecutor(joinSegWriter));
}
OpenExecutorPtr OpenExecutorChainCreator::CreateReopenPartitionReaderExecutor(bool hasPreload)
{
    return OpenExecutorPtr(new ReopenPartitionReaderExecutor(hasPreload, mPartitionName));
}
OpenExecutorPtr OpenExecutorChainCreator::CreateReleaseReaderExecutor()
{
    return OpenExecutorPtr(new ReleasePartitionReaderExecutor());
}
}} // namespace indexlib::partition
