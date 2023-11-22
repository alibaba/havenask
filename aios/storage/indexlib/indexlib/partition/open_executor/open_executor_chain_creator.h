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

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/open_executor/open_executor.h"
#include "indexlib/partition/open_executor/open_executor_chain.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);
DECLARE_REFERENCE_CLASS(partition, JoinSegmentWriter);
DECLARE_REFERENCE_CLASS(partition, IndexPartition);

namespace indexlib { namespace partition {

class OpenExecutorChainCreator
{
    typedef std::shared_ptr<autil::ScopedLock> ScopedLockPtr;

public:
    OpenExecutorChainCreator(std::string partitionName, IndexPartition* partition)
        : mPartitionName(partitionName)
        , mPartition(partition)
    {
    }
    virtual ~OpenExecutorChainCreator() {}
    virtual OpenExecutorChainPtr
    CreateReopenExecutorChain(config::IndexPartitionSchemaPtr schema, file_system::IFileSystemPtr fileSystem,
                              util::PartitionMemoryQuotaControllerPtr partitionMemController,
                              index_base::PartitionDataHolder& partitionDataHolder,
                              index_base::Version loadedIncVersion, index_base::Version& onDiskVersion,
                              ScopedLockPtr& scopedLock, autil::RecursiveThreadMutex& dataLock,
                              autil::ThreadMutex& dumpLock, config::IndexPartitionOptions& options,
                              OnlinePartitionMetricsPtr onlinePartMetrics, bool enableAsyncDump, int64_t redoTime);
    virtual OpenExecutorChainPtr
    CreateForceReopenExecutorChain(config::IndexPartitionSchemaPtr schema, file_system::IFileSystemPtr fileSystem,
                                   util::PartitionMemoryQuotaControllerPtr partitionMemController,
                                   index_base::PartitionDataHolder& partitionDataHolder,
                                   index_base::Version loadedIncVersion, index_base::Version& onDiskVersion,
                                   autil::RecursiveThreadMutex& dataLock, config::IndexPartitionOptions& options,
                                   OnlinePartitionMetricsPtr onlinePartMetrics, bool enableAsyncDump);
    OpenExecutorChainPtr CreateReopenPartitionReaderChain(bool hasPreload);

protected:
    virtual OpenExecutorChainPtr createOpenExecutorChain()
    {
        OpenExecutorChainPtr chain(new OpenExecutorChain);
        return chain;
    }
    virtual OpenExecutorPtr CreateReopenPartitionReaderExecutor(bool hasPreload);

private:
    bool CanOptimizedReopen(config::IndexPartitionOptions& options, config::IndexPartitionSchemaPtr schema,
                            index_base::Version& onDiskVersion) const;
    JoinSegmentWriterPtr
    CreateJoinSegmentWriter(config::IndexPartitionSchemaPtr schema, file_system::IFileSystemPtr fileSystem,
                            util::PartitionMemoryQuotaControllerPtr partitionMemController,
                            index_base::PartitionDataHolder& partitionDataHolder, index_base::Version loadedIncVersion,
                            const index_base::Version& onDiskVersion, config::IndexPartitionOptions& options,
                            OnlinePartitionMetricsPtr onlinePartMetrics, bool isForceReopen) const;

    OpenExecutorPtr CreateLockExecutor(autil::ThreadMutex* lock);
    OpenExecutorPtr CreateUnLockExecutor(autil::ThreadMutex* lock);
    OpenExecutorPtr CreateRedoAndLockExecutor(autil::ThreadMutex* lock, int64_t redoTime);

    OpenExecutorPtr CreateScopedUnlockExecutor(ScopedLockPtr& scopedLock, autil::ThreadMutex& dataLock);
    OpenExecutorPtr CreatePreloadExecutor();
    OpenExecutorPtr CreatePrepatchExecutor(autil::RecursiveThreadMutex* dataLock);
    OpenExecutorPtr CreatePreJoinExecutor(const JoinSegmentWriterPtr& joinSegWriter);
    OpenExecutorPtr CreateScopedLockExecutor(ScopedLockPtr& scopedLock, autil::ThreadMutex& dataLock);
    OpenExecutorPtr CreateReclaimRtIndexExecutor(bool isInplaceReclaim = false);
    OpenExecutorPtr CreateReclaimRtSegmentsExecutor(bool reclaimBuildingSegment = true);
    OpenExecutorPtr CreateGenerateJoinSegmentExecutor(const JoinSegmentWriterPtr& joinSegWriter);
    OpenExecutorPtr CreateSwitchBranchExecutor();
    OpenExecutorPtr CreateReleaseReaderExecutor();
    OpenExecutorPtr CreateDumpSegmentExecutor(bool reInitReaderAndWriter = true);

private:
    std::string mPartitionName;
    IndexPartition* mPartition;

private:
    friend class OnlinePartitionTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OpenExecutorChainCreator);
}} // namespace indexlib::partition
