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
#include "indexlib/document/index_locator.h"
#include "indexlib/index_base/common_branch_hinter_option.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);
DECLARE_REFERENCE_CLASS(file_system, FileBlockCacheContainer);
DECLARE_REFERENCE_CLASS(file_system, FileBlockCache);
DECLARE_REFERENCE_CLASS(file_system, FileSystemMetrics);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, TaskScheduler);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, MemoryReserver);
DECLARE_REFERENCE_CLASS(util, QuotaControl);
DECLARE_REFERENCE_CLASS(util, PartitionMemoryQuotaController);
DECLARE_REFERENCE_CLASS(partition, PartitionWriter);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(partition, FlushedLocatorContainer);
DECLARE_REFERENCE_CLASS(partition, TableReaderContainerUpdater);
DECLARE_REFERENCE_CLASS(document, Locator);
DECLARE_REFERENCE_CLASS(index_base, BranchFS);

namespace indexlib { namespace partition {

class IndexPartition
{
public:
    enum IndexPhase {
        FORCE_OPEN = 7,
        FORCE_REOPEN = 6,
        SWITCH_FLUSH_RT = 5,
        RECLAIM_READER_MEM = 4,
        NORMAL_REOPEN = 3,
        REOPEN_RT = 2,
        OPENING = 1,
        PENDING = 0,
    };

    class IndexPhaseGuard
    {
    public:
        IndexPhaseGuard(volatile IndexPhase& argument, IndexPhase phase) : mArgument(argument) { mArgument = phase; }

        ~IndexPhaseGuard() { mArgument = PENDING; }

    private:
        volatile IndexPhase& mArgument;
    };

    enum OpenStatus {
        OS_OK = 0,
        OS_FAIL = 1,
        OS_LACK_OF_MEMORY = 2,
        OS_FILEIO_EXCEPTION = 3,
        OS_INDEXLIB_EXCEPTION = 4,
        OS_UNKNOWN_EXCEPTION = 5,
        OS_INCONSISTENT_SCHEMA = 6,
        OS_FORCE_REOPEN = 10,
    };

    enum MemoryStatus {
        MS_OK = 0,
        MS_REACH_MAX_RT_INDEX_SIZE = 1,
        MS_REACH_TOTAL_MEM_LIMIT = 2,
        MS_REACH_LOW_WATERMARK_MEM_LIMIT = 3,
        MS_REACH_HIGH_WATERMARK_MEM_LIMIT = 4,
        MS_UNKOWN = 5
    };

public:
    IndexPartition(const IndexPartitionResource& partitionResource);
    virtual ~IndexPartition();

public:
    virtual OpenStatus Open(const std::string& primaryDir, const std::string& secondaryDir,
                            const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                            versionid_t versionId = INVALID_VERSIONID);

    // reopenVersionId = INVALID_VERSIONID, will use ondisk newest version
    virtual OpenStatus ReOpen(bool forceReopen, versionid_t reopenVersionId = INVALID_VERSIONID) = 0;

    virtual void ReOpenNewSegment() = 0;

    virtual void Close();

    virtual partition::PartitionWriterPtr GetWriter() const = 0;
    virtual IndexPartitionReaderPtr GetReader() const = 0;
    virtual IndexPartitionReaderPtr GetReader(versionid_t versionId) const;
    virtual MemoryStatus CheckMemoryStatus() const { return MS_OK; }
    virtual void ExecuteBuildMemoryControl() { return; }

    virtual config::IndexPartitionSchemaPtr GetSchema() const
    {
        autil::ScopedLock lock(mDataLock);
        return mSchema;
    }

    virtual bool CleanIndexFiles(const std::vector<versionid_t>& keepVersionIds)
    {
        assert(false);
        return false;
    };

    virtual bool CleanUnreferencedIndexFiles(const std::set<std::string>& toKeepFiles) { return false; };

    static bool CleanIndexFiles(const std::string& primaryPath, const std::string& secondaryPath,
                                const std::vector<versionid_t>& keepVersionIds);

    virtual void Lock() const AUTIL_ACQUIRE(mDataLock) { mDataLock.lock(); }
    virtual void UnLock() const AUTIL_RELEASE(mDataLock) { mDataLock.unlock(); }

    // for online partition
    virtual void AddReaderUpdater(TableReaderContainerUpdater* readerUpdater) {};
    virtual void RemoveReaderUpdater(TableReaderContainerUpdater* readerUpdater) {};
    virtual void AddVirtualAttributes(const config::AttributeConfigVector& mainVirtualAttrConfigs,
                                      const config::AttributeConfigVector& subVirtualAttrConfigs)
    {
    }
    virtual util::MemoryReserverPtr
    ReserveVirtualAttributesResource(const config::AttributeConfigVector& mainVirtualAttrConfigs,
                                     const config::AttributeConfigVector& subVirtualAttrConfigs)
    {
        return util::MemoryReserverPtr();
    }

public:
    const config::IndexPartitionOptions& GetOptions() const
    {
        autil::ScopedLock lock(mDataLock);
        return mOptions;
    }

    const std::string& GetRootPath() const
    {
        return mOpenIndexSecondaryDir.empty() ? mOpenIndexPrimaryDir : mOpenIndexSecondaryDir;
    };
    const file_system::DirectoryPtr& GetRootDirectory() const;
    file_system::DirectoryPtr GetFileSystemRootDirectory() const;

    autil::ThreadMutex* GetDataLock() { return &mDataLock; }

    const index_base::IndexFormatVersion& GetIndexFormatVersion() const
    {
        autil::ScopedLock lock(mDataLock);
        index_base::PartitionDataPtr partitionData = mPartitionDataHolder.Get();
        assert(partitionData);
        return partitionData->GetIndexFormatVersion();
    }

    index_base::PartitionMeta GetPartitionMeta() const;

    index_base::PartitionDataPtr GetPartitionData() const
    {
        index_base::PartitionDataPtr partitionData = mPartitionDataHolder.Get();
        return partitionData;
    }

    int64_t GetUsedMemory() const;

    void SetBuildMemoryQuotaControler(const util::QuotaControlPtr& buildMemoryQuotaControler);

    const util::CounterMapPtr& GetCounterMap() const { return mCounterMap; }

    document::Locator GetLastFlushedLocator();

    bool HasFlushingLocator();

    virtual std::string GetLastLocator() const
    {
        index_base::PartitionDataPtr partitionData = mPartitionDataHolder.Get();
        return partitionData ? partitionData->GetLastLocator() : std::string("");
    }

    virtual int64_t GetRtSeekTimestampFromIncVersion() const { return INVALID_TIMESTAMP; }

    virtual uint64_t GetPartitionIdentifier() const { return 0; }

    bool NeedReload() const { return mNeedReload.load(std::memory_order_relaxed); }

    virtual void SetForceSeekInfo(const std::pair<int64_t, int64_t>& forceSeekInfo) { mForceSeekInfo = forceSeekInfo; }

    virtual void RedoOperations() {}

    virtual document::SrcSignature GetSrcSignature() const { return document::SrcSignature(); }

public:
    // for test
    file_system::FileSystemMetrics TEST_GetFileSystemMetrics() const;

    const file_system::FileBlockCachePtr& TEST_GetFileBlockCache() const;

    const std::string& GetSecondaryRootPath() const
    {
        return mOpenIndexSecondaryDir.empty() ? mOpenIndexPrimaryDir : mOpenIndexSecondaryDir;
    }

    file_system::IFileSystemPtr GetFileSystem() const { return mFileSystem; }

    index_base::BranchFS* GetBranchFileSystem() const { return mBranchFileSystem.get(); }

protected:
    virtual bool NotAllowToModifyRootPath() const = 0;
    virtual void CheckParam(const config::IndexPartitionSchemaPtr& schema,
                            const config::IndexPartitionOptions& options) const = 0;
    bool IsEmptyDir(const std::string& root, const config::IndexPartitionSchemaPtr& schema) const;

    virtual file_system::IFileSystemPtr CreateFileSystem(const std::string& primaryDir,
                                                         const std::string& secondaryDir);
    void CleanRootDir(const std::string& root, const config::IndexPartitionSchemaPtr& schema,
                      file_system::FenceContext* fenceContext);

    void CheckPartitionMeta(const config::IndexPartitionSchemaPtr& schema,
                            const index_base::PartitionMeta& partitionMeta) const;
    void Reset();
    void InitEmptyIndexDir(file_system::FenceContext* fenceContext);

protected:
    std::string mPartitionName;

    util::PartitionMemoryQuotaControllerPtr mPartitionMemController;

    std::string mOpenIndexPrimaryDir;
    std::string mOpenIndexSecondaryDir;
    config::IndexPartitionSchemaPtr mOpenSchema;
    config::IndexPartitionOptions mOpenOptions;

    mutable autil::RecursiveThreadMutex mDataLock;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

    file_system::FileBlockCacheContainerPtr mFileBlockCacheContainer;
    file_system::IFileSystemPtr mFileSystem;
    index_base::PartitionDataHolder mPartitionDataHolder;
    std::unique_ptr<index_base::BranchFS> mBranchFileSystem;
    index_base::CommonBranchHinterOption mHinterOption;

    util::TaskSchedulerPtr mTaskScheduler;
    util::MetricProviderPtr mMetricProvider;
    util::CounterMapPtr mCounterMap;

    partition::FlushedLocatorContainerPtr mFlushedLocatorContainer;
    std::string mIndexPluginPath;
    plugin::PluginManagerPtr mPluginManager;
    std::atomic<bool> mNeedReload;

    std::pair<int64_t, int64_t> mForceSeekInfo;
    file_system::DirectoryPtr mRootDirectory;

private:
    friend class IndexPartitionTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPartition);
}} // namespace indexlib::partition
