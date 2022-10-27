#ifndef __INDEXLIB_PARTITION_INDEX_PARTITION_H
#define __INDEXLIB_PARTITION_INDEX_PARTITION_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"
#include "indexlib/index_define.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/partition_data.h"
#include <memory>

DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(file_system, IndexlibFileSystem);
DECLARE_REFERENCE_CLASS(file_system, FileBlockCache);
DECLARE_REFERENCE_CLASS(file_system, IndexlibFileSystemMetrics);
DECLARE_REFERENCE_CLASS(util, TaskScheduler);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, MemoryQuotaController);
DECLARE_REFERENCE_CLASS(util, QuotaControl);
DECLARE_REFERENCE_CLASS(util, PartitionMemoryQuotaController);
DECLARE_REFERENCE_CLASS(partition, PartitionWriter);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionResource);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(partition, FlushedLocatorContainer);
DECLARE_REFERENCE_CLASS(partition, TableReaderContainerUpdater);
DECLARE_REFERENCE_CLASS(document, Locator);

IE_NAMESPACE_BEGIN(partition);

class IndexPartition
{
public:
    enum IndexPhase
    {
        FORCE_OPEN = 7,
        FORCE_REOPEN = 6,
        SWITCH_FLUSH_RT = 5,
        RECLAIM_READER_MEM = 4,
        NORMAL_REOPEN = 3,
        REOPEN_RT = 2,
        OPENING  = 1,
        PENDING  = 0,
    };

    class IndexPhaseGuard
    {
    public:
        IndexPhaseGuard(volatile IndexPhase &argument, IndexPhase phrase)
            : mArgument(argument)
        {
            mArgument = phrase;
        }

        ~IndexPhaseGuard()
        {
            mArgument = PENDING;
        }
    private:
        volatile IndexPhase &mArgument;
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
        MS_UNKOWN = 3
    };

public:
    IndexPartition();
    IndexPartition(const std::string& partitionName,
                   const IndexPartitionResource& partitionResource);

    IndexPartition(const util::MemoryQuotaControllerPtr& memQuotaController,
                   const std::string& partitionName);
    virtual ~IndexPartition();

public:
    virtual OpenStatus Open(const std::string& primaryDir,
                            const std::string& secondaryDir,
                            const config::IndexPartitionSchemaPtr& schema,
                            const config::IndexPartitionOptions& options,
                            versionid_t versionId = INVALID_VERSION);
    
    // reopenVersionId = INVALID_VERSION, will use ondisk newest version
    virtual OpenStatus ReOpen(
            bool forceReopen, 
            versionid_t reopenVersionId = INVALID_VERSION) = 0;

    virtual void ReOpenNewSegment() = 0;

    virtual void Close();

    virtual partition::PartitionWriterPtr GetWriter() const = 0;
    virtual IndexPartitionReaderPtr GetReader() const = 0;
    virtual MemoryStatus CheckMemoryStatus() const
    { assert(false); return MS_OK; }

    virtual config::IndexPartitionSchemaPtr GetSchema() const
    { autil::ScopedLock lock(mDataLock); return mSchema; }

    virtual bool CleanIndexFiles(const std::vector<versionid_t>& keepVersionIds)
    { assert(false); return false; };
    static bool CleanIndexFiles(const std::string& primaryPath,
                                const std::string& secondaryPath,
                                const std::vector<versionid_t>& keepVersionIds);
    virtual void AddReaderUpdater(TableReaderContainerUpdater* readerUpdater) {};
    virtual void RemoveReaderUpdater(TableReaderContainerUpdater* readerUpdater) {};

    virtual void Lock() const { mDataLock.lock(); } 
    virtual void UnLock() const { mDataLock.unlock(); } 

public:
    const config::IndexPartitionOptions& GetOptions() const
    { autil::ScopedLock lock(mDataLock); return mOptions; }

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

    uint64_t GetReaderHashId() const { return mReaderHashId; }
    void SetReaderHashId(uint64_t hashId) { mReaderHashId = hashId; }

    document::Locator GetLastFlushedLocator();

    bool HasFlushingLocator();

    virtual std::string GetLastLocator() const
    {
        index_base::PartitionDataPtr partitionData = mPartitionDataHolder.Get();
        return partitionData ? partitionData->GetLastLocator() : std::string("");
    }

    virtual int64_t GetRtSeekTimestampFromIncVersion() const
    {
        return INVALID_TIMESTAMP;
    }
    
    virtual uint64_t GetPartitionIdentifier() const
    {
        return 0;
    }

    bool NeedReload() const { return mNeedReload.load(std::memory_order_relaxed); }

    virtual void SetForceSeekInfo(const std::pair<int64_t, int64_t>& forceSeekInfo)
    {
        mForceSeekInfo = forceSeekInfo;
    }

public:
    //for test
    file_system::IndexlibFileSystemMetrics TEST_GetFileSystemMetrics() const;
    
    const file_system::FileBlockCachePtr& TEST_GetFileBlockCache() const
    {
        return mFileBlockCache;
    }

    const std::string& GetSecondaryRootPath() const
    { return mOpenIndexSecondaryDir.empty() ?
            mOpenIndexPrimaryDir : mOpenIndexSecondaryDir; }

protected: 
    virtual file_system::IndexlibFileSystemPtr CreateFileSystem(
        const std::string& primaryDir, const std::string& secondaryDir);

    void CheckPartitionMeta(const config::IndexPartitionSchemaPtr& schema,
                            const index_base::PartitionMeta& partitionMeta) const;
    void Reset();

protected:
    volatile uint64_t mReaderHashId;
    std::string mPartitionName;
    util::MemoryQuotaControllerPtr mMemQuotaController;
    util::QuotaControlPtr mBuildMemoryQuotaController;

    std::string mOpenIndexPrimaryDir;
    std::string mOpenIndexSecondaryDir;
    config::IndexPartitionSchemaPtr mOpenSchema;
    config::IndexPartitionOptions mOpenOptions;
    
    mutable autil::RecursiveThreadMutex mDataLock;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

    file_system::FileBlockCachePtr mFileBlockCache;
    file_system::IndexlibFileSystemPtr mFileSystem;
    index_base::PartitionDataHolder mPartitionDataHolder;
    
    util::PartitionMemoryQuotaControllerPtr mPartitionMemController;
    util::TaskSchedulerPtr mTaskScheduler;
    misc::MetricProviderPtr mMetricProvider;
    util::CounterMapPtr mCounterMap;

    partition::FlushedLocatorContainerPtr mFlushedLocatorContainer;
    std::string mIndexPluginPath;
    plugin::PluginManagerPtr mPluginManager;
    std::atomic<bool> mNeedReload;

    std::pair<int64_t, int64_t> mForceSeekInfo;

private:
    friend class IndexPartitionTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPartition);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_INDEX_PARTITION_H

