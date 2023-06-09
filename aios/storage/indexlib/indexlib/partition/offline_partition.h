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
#ifndef __INDEXLIB_OFFLINE_PARTITION_H
#define __INDEXLIB_OFFLINE_PARTITION_H

#include <memory>

#include "autil/Thread.h"
#include "autil/ThreadPool.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/segment/in_memory_segment_cleaner.h"
#include "indexlib/util/metrics/Monitor.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(partition, OfflinePartitionWriter);
DECLARE_REFERENCE_CLASS(partition, OfflinePartitionReader);
DECLARE_REFERENCE_CLASS(partition, InMemorySegmentContainer);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentContainer);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);

namespace indexlib { namespace partition {

class OfflinePartition : public IndexPartition
{
public:
    // only for ut
    OfflinePartition();

    // only for read-only partition (using in extract doc task and tools)
    OfflinePartition(bool branchNameLegacy);

    // used for normal
    OfflinePartition(const IndexPartitionResource& partitionResource);

    ~OfflinePartition();

public:
    OpenStatus Open(const std::string& primaryDir, const std::string& secondaryDir,
                    const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                    versionid_t versionId = INVALID_VERSION) override;

    OpenStatus ReOpen(bool forceReopen, versionid_t reopenVersionId = INVALID_VERSION) override;

    void ReOpenNewSegment() override;
    void Close() override;
    PartitionWriterPtr GetWriter() const override;
    IndexPartitionReaderPtr GetReader() const override;

private:
    file_system::IFileSystemPtr CreateFileSystem(const std::string& primaryDir,
                                                 const std::string& secondaryDir) override;

private:
    OpenStatus DoOpen(const std::string& root, const config::IndexPartitionSchemaPtr& schema,
                      const config::IndexPartitionOptions& options, versionid_t versionId);
    void InitPartitionData(index_base::Version& version, const util::CounterMapPtr& counterMap,
                           const plugin::PluginManagerPtr& pluginManager);
    void RewriteLoadConfigs(config::IndexPartitionOptions& options) const;
    void InitWriter(const index_base::PartitionDataPtr& partData);

    void CheckParam(const config::IndexPartitionSchemaPtr& schema,
                    const config::IndexPartitionOptions& options) const override;
    PartitionModifierPtr CreateModifier(const index_base::PartitionDataPtr& partData);
    // report metrics and release memory
    void BackGroundThread();
    void PushToReleasePool(index_base::InMemorySegmentPtr& inMemSegment);

    // virtual for test
    virtual OfflinePartitionWriterPtr CreatePartitionWriter();

    void CheckSortedKKVIndex(const index_base::PartitionDataPtr& partData);

    util::SearchCachePartitionWrapperPtr CreateSearchCacheWrapper() const;

    bool IsFirstIncParallelInstance() const
    {
        return mParallelBuildInfo.IsParallelBuild() && (mParallelBuildInfo.instanceId == 0);
    }

    bool NotAllowToModifyRootPath() const override
    {
        return mParallelBuildInfo.IsParallelBuild() && (!IsFirstIncParallelInstance() || mHinterOption.branchId);
    }

    void MountPatchIndex();

    void ReportTemperatureIndexSize(const index_base::Version& version) const;

private:
    OfflinePartitionWriterPtr mWriter;
    mutable partition::OfflinePartitionReaderPtr mReader;
    volatile bool mClosed;
    autil::ThreadPtr mBackGroundThreadPtr;
    DumpSegmentContainerPtr mDumpSegmentContainer;
    InMemorySegmentCleaner mInMemSegCleaner;
    index_base::ParallelBuildInfo mParallelBuildInfo;
    mutable autil::ThreadMutex mReaderLock;
    PartitionRange mPartitionRange;

    IE_DECLARE_METRIC(oldInMemorySegmentMemoryUse);
    IE_DECLARE_METRIC(fixedCostMemoryRatio);
    IE_DECLARE_METRIC(buildMemoryQuota);
    IE_DECLARE_METRIC(temperatureIndexSize);

private:
    friend class OfflinePartitionTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflinePartition);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OFFLINE_PARTITION_H
