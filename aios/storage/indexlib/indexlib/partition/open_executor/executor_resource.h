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
#include "indexlib/document/index_locator.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(partition, OnlinePartitionReader);
DECLARE_REFERENCE_CLASS(partition, OnlinePartitionWriter);
DECLARE_REFERENCE_CLASS(partition, OnlinePartitionMetrics);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentContainer);
DECLARE_REFERENCE_CLASS(partition, ReaderContainer);
DECLARE_REFERENCE_CLASS(partition, PartitionResourceCalculator);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(index_base, PartitionDataHolder);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);
DECLARE_REFERENCE_CLASS(common, ExecutorScheduler);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);
DECLARE_REFERENCE_CLASS(util, PartitionMemoryQuotaController);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

namespace indexlib { namespace partition {

struct ExecutorResource {
public:
    index_base::Version& mLoadedIncVersion;
    OnlinePartitionReaderPtr& mReader;
    autil::ThreadMutex& mReaderLock;
    OnlinePartitionWriterPtr& mWriter;
    index_base::PartitionDataHolder& mPartitionDataHolder;
    OnlinePartitionMetrics& mOnlinePartMetrics;
    config::IndexPartitionOptions& mOptions;
    std::atomic<bool>& mNeedReload;

    index_base::Version mIncVersion;
    file_system::IFileSystemPtr mFileSystem;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionSchemaPtr mRtSchema;
    ReaderContainerPtr mReaderContainer;
    OnlinePartitionReaderPtr mPreloadReader;
    DumpSegmentContainerPtr mDumpSegmentContainer;
    autil::RecursiveThreadMutex* mCleanerLock;
    common::ExecutorSchedulerPtr mResourceCleaner;
    PartitionResourceCalculatorPtr mResourceCalculator;
    util::PartitionMemoryQuotaControllerPtr mPartitionMemController;
    util::SearchCachePartitionWrapperPtr mSearchCache;
    util::CounterMapPtr mCounterMap;
    bool mForceReopen;
    bool mNeedInheritInMemorySegment;
    int64_t mLatestNeedSkipIncTs;
    plugin::PluginManagerPtr mPluginManager;
    IndexPartition::IndexPhase mIndexPhase;
    document::SrcSignature mSrcSignature;

    index_base::PartitionDataPtr mSnapshotPartitionData;
    bool mNeedReportTemperatureAccess;
    // for branch reopen
    OnlinePartitionReaderPtr mBranchReader;
    index_base::PartitionDataHolder mBranchPartitionDataHolder;

public:
    ExecutorResource(index_base::Version& loadedIncVersion, OnlinePartitionReaderPtr& reader,
                     autil::ThreadMutex& readerLock, OnlinePartitionWriterPtr& writer,
                     index_base::PartitionDataHolder& partDataHolder, OnlinePartitionMetrics& metrics,
                     std::atomic<bool>& needReload, config::IndexPartitionOptions& options,
                     int64_t latestNeedSkipIncTs = INVALID_TIMESTAMP,
                     IndexPartition::IndexPhase indexPhase = IndexPartition::PENDING)
        : mLoadedIncVersion(loadedIncVersion)
        , mReader(reader)
        , mReaderLock(readerLock)
        , mWriter(writer)
        , mPartitionDataHolder(partDataHolder)
        , mOnlinePartMetrics(metrics)
        , mOptions(options)
        , mNeedReload(needReload)
        , mCleanerLock(NULL)
        , mForceReopen(false)
        , mNeedInheritInMemorySegment(true)
        , mLatestNeedSkipIncTs(latestNeedSkipIncTs)
        , mIndexPhase(indexPhase)
        , mNeedReportTemperatureAccess(false)
    {
    }
};

DEFINE_SHARED_PTR(ExecutorResource);
}} // namespace indexlib::partition
