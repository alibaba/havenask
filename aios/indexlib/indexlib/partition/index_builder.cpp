#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/partition_writer.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/merger/parallel_partition_data_merger.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/partition/on_disk_index_cleaner.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/memory_control/quota_control.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/partition/index_partition_creator.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/partition/custom_online_partition.h"
#include <beeper/beeper.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(merger);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IndexBuilder);

IndexBuilder::IndexBuilder(const string& rootDir,
                           const IndexPartitionOptions& options,
                           const IndexPartitionSchemaPtr& schema,
                           const util::QuotaControlPtr& memoryQuotaControl,
                           MetricProviderPtr metricProvider,
                           const std::string& indexPluginPath,
                           const PartitionRange &range)
    : mDataLock(NULL)
    , mOptions(options)
    , mSchema(schema)
    , mMemoryQuotaControl(memoryQuotaControl)
    , mBuilderMetrics(metricProvider)
    , mMetricProvider(metricProvider)
    , mRootDir(rootDir)
    , mIsLegacyIndexVersion(false)
    , mSupportBuild(true)
    , mStatus(INITIALIZING)
    , mIndexPluginPath(indexPluginPath)
    , mPartitionRange(range)
    , mLastConsumedMessageCount(0)
{
    assert(mMemoryQuotaControl);
    IE_LOG(INFO, "memory quota for index builder is %lu MB",
           mMemoryQuotaControl->GetLeftQuota() / (1024 * 1024));
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
            "memory quota for index builder is %lu MB",
            mMemoryQuotaControl->GetLeftQuota() / (1024 * 1024));
    mOptions.SetIsOnline(false);
    mOptions.GetOfflineConfig().enableRecoverIndex = true;
}

IndexBuilder::IndexBuilder(const string& rootDir,
                           const ParallelBuildInfo& parallelInfo,
                           const IndexPartitionOptions& options,
                           const IndexPartitionSchemaPtr& schema,
                           const util::QuotaControlPtr& memoryQuotaControl,
                           MetricProviderPtr metricProvider,
                           const std::string& indexPluginPath,
                           const PartitionRange &range)
    : mDataLock(NULL)
    , mOptions(options)
    , mSchema(schema)
    , mMemoryQuotaControl(memoryQuotaControl)
    , mBuilderMetrics(metricProvider)
    , mMetricProvider(metricProvider)
    , mRootDir(rootDir)
    , mIsLegacyIndexVersion(false)
    , mSupportBuild(true)
    , mStatus(INITIALIZING)
    , mIndexPluginPath(indexPluginPath)
    , mParallelInfo(parallelInfo)
    , mPartitionRange(range)      
{
    assert(mMemoryQuotaControl);
    IE_LOG(INFO, "memory quota for index builder is %lu MB",
           mMemoryQuotaControl->GetLeftQuota() / (1024 * 1024));
    mOptions.SetIsOnline(false);
    mOptions.GetOfflineConfig().enableRecoverIndex = true;
}


// use existing index partition
IndexBuilder::IndexBuilder(const IndexPartitionPtr& indexPartitionPtr,
                           const util::QuotaControlPtr& memoryQuotaControl,
                           MetricProviderPtr metricProvider,
                           const PartitionRange &range)
    : mDataLock(NULL)
    , mMemoryQuotaControl(memoryQuotaControl)
    , mBuilderMetrics(metricProvider)
    , mMetricProvider(metricProvider)
    , mIsLegacyIndexVersion(false)
    , mStatus(INITIALIZING)
    , mPartitionRange(range)      
{
    assert(mMemoryQuotaControl);
    IE_LOG(INFO, "memory quota for index builder is %lu MB",
           mMemoryQuotaControl->GetLeftQuota() / (1024 * 1024));
    mIndexPartition = indexPartitionPtr;
    mSupportBuild = SupportOnlineBuild(indexPartitionPtr->GetSchema());
    mIndexPartition->SetBuildMemoryQuotaControler(mMemoryQuotaControl);
}

IndexBuilder::~IndexBuilder()
{
    mIndexPartitionWriter.reset();
    mIndexPartition.reset();
}

bool IndexBuilder::Init()
{
    if (!mIndexPartition)
    {
        assert(mOptions.IsOffline());
        // TODO: give partitionName and totalQuota(unused till now)
        // so ugly, QuotaControl(Build memory), MemoryQuotaController(total), PartitionMemoryQuotaController(partition)
        if (!CreateInstanceDirectoryForParallelInc())
        {
            return false;
        }
        // TODO: use docker mem-limit as totalQuotaForPartition
        int64_t totalQuotaForPartition = numeric_limits<int64_t>::max();
        util::MemoryQuotaControllerPtr memController(new util::MemoryQuotaController(totalQuotaForPartition));
        IndexPartitionResource partitionResource;
        partitionResource.memoryQuotaController = memController;
        partitionResource.metricProvider = mMetricProvider;
        partitionResource.indexPluginPath = mIndexPluginPath;
        partitionResource.parallelBuildInfo = mParallelInfo;
        partitionResource.taskScheduler.reset(new util::TaskScheduler());
        partitionResource.range = mPartitionRange;
        mIndexPartition =
            CreateIndexPartition(mSchema, "", mOptions, partitionResource);
        mIndexPartition->SetBuildMemoryQuotaControler(mMemoryQuotaControl);
        IndexPartition::OpenStatus os = 
            mIndexPartition->Open(mRootDir, "", mSchema, mOptions);
        if (os != IndexPartition::OS_OK)
        {
            IE_LOG(ERROR, "open partition from [%s] failed, OpenStatue [%d]",
                   mRootDir.c_str(), os);
            ERROR_COLLECTOR_LOG(ERROR, "open partition from [%s] failed, OpenStatue [%d]",
                   mRootDir.c_str(), os);
            mIndexPartition.reset();
            return false;
        }
    }

    mDataLock = mIndexPartition->GetDataLock();
    ScopedLock lock(*mDataLock);

    mOptions = mIndexPartition->GetOptions();
    mSchema = mIndexPartition->GetSchema();

    mBuilderMetrics.RegisterMetrics(mSchema->GetTableType());

    mIndexPartitionWriter = mIndexPartition->GetWriter();
    mPartitionMeta = mIndexPartition->GetPartitionMeta();

    mIsLegacyIndexVersion = 
        mIndexPartition->GetIndexFormatVersion().IsLegacyFormat();

    if (mOptions.IsOnline())
    {
        const DirectoryPtr& rootDirectory = 
            mIndexPartition->GetRootDirectory();
        assert(rootDirectory);
        mRootDir = rootDirectory->GetPath();
    }
    mStatus = mIndexPartitionWriter ? BUILDING : INITIALIZING;
    return true;
}

bool IndexBuilder::CreateInstanceDirectoryForParallelInc()
{
    if (!mParallelInfo.IsParallelBuild())
    {
        IE_LOG(INFO, "Init builder in partition path [%s]", mRootDir.c_str());
        return true;
    }

    if (BuildConfig::BP_INC != mOptions.GetBuildConfig(false).buildPhase)
    {
        IE_LOG(ERROR, "parallel build only support inc, not for [%d]",
               mOptions.GetBuildConfig(false).buildPhase);
        ERROR_COLLECTOR_LOG(ERROR, "parallel build only support inc, not for [%d]",
                            mOptions.GetBuildConfig(false).buildPhase);
        return false;
    }
    FileSystemWrapper::MkDirIfNotExist(mParallelInfo.GetParallelPath(mRootDir));
    string workDir = mParallelInfo.GetParallelInstancePath(mRootDir);
    FileSystemWrapper::MkDirIfNotExist(workDir);
    IE_LOG(INFO, "Init inc builder in parallel instance path [%s]", workDir.c_str());
    return true;
}

bool IndexBuilder::SupportOnlineBuild(const IndexPartitionSchemaPtr& schema)
{
    if (schema->GetTableType() == tt_customized)
    {
        return true;
    }
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    if (!indexSchema->HasPrimaryKeyIndex())
    {
        return true;
    }
    return indexSchema->GetPrimaryKeyIndexType() != it_trie;
}

IndexPartitionPtr IndexBuilder::CreateIndexPartition(const IndexPartitionSchemaPtr& schema,
                                                     const string& partitionName,
                                                     const IndexPartitionOptions& options,
                                                     const IndexPartitionResource& indexPartitionResource)
{
    assert(schema);
    if (schema->GetTableType() == tt_customized) {
        return IndexPartitionCreator::CreateCustomIndexPartition(
            partitionName, options, indexPartitionResource);
    }
    return IndexPartitionCreator::Create(
            partitionName, options, indexPartitionResource);
}

bool IndexBuilder::Build(const DocumentPtr& doc)
{
    ScopedLock lock(*mDataLock);
    if (mIndexPartition->NeedReload())
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "load table [%s] error, need reload",
                             mSchema->GetSchemaName().c_str());
    }
    if (unlikely(mStatus != BUILDING))
    {
        if (unlikely(!mSupportBuild))
        {
            IE_LOG(WARN, "Not support build, drop doc");
            ERROR_COLLECTOR_LOG(WARN, "Not support build, drop doc");
            return false;
        }
        if (mStatus != INITIALIZING)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "table [%s] still in status[%d]",
                    mSchema->GetSchemaName().c_str(), mStatus.load());
        }
        else
        {
            assert(!mIndexPartitionWriter);
            mIndexPartition->ReOpenNewSegment();
            mIndexPartitionWriter = mIndexPartition->GetWriter();
            mStatus = BUILDING;
        }
    }
    if (!doc)
    {
        IE_LOG(WARN, "empty document");
        ERROR_COLLECTOR_LOG(WARN, "empty document");
        mBuilderMetrics.ReportMetrics(0, doc);
        return false;
    }
    assert(mIndexPartitionWriter);
    if (mIndexPartitionWriter->NeedRewriteDocument(doc))
    {
        mIndexPartitionWriter->RewriteDocument(doc);
    }

    //mTTLDecoder.SetDocumentTTL(doc);

    if (!BuildDocument(doc))
    {
        if (mIndexPartitionWriter->GetStatus() == misc::NO_SPACE)
        {
            BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                    "DumpSegment for partition writer reach [misc::NO_SPACE] status");
            
            DumpSegment();
            return BuildDocument(doc);
        }
        return false;
    }
    return true;
}

bool IndexBuilder::BuildDocument(const DocumentPtr& doc)
{
    if (mSchema->GetTableType() == tt_index)
    {
        NormalDocumentPtr normalDoc = DYNAMIC_POINTER_CAST(NormalDocument, doc);
        if (normalDoc && normalDoc->GetSchemaVersionId() != mSchema->GetSchemaVersionId())
        {
            IE_INTERVAL_LOG2(10, INFO, "doc schema version [%d] not match with schema version [%d]",
                    normalDoc->GetSchemaVersionId(), mSchema->GetSchemaVersionId());
            mBuilderMetrics.ReportSchemaVersionMismatch();
        }
    }
    
    bool ret = mIndexPartitionWriter->BuildDocument(doc);
    if (ret && mIndexPartitionWriter->NeedDump(mMemoryQuotaControl->GetLeftQuota()))
    {
        DumpSegment();
    }
    mLastConsumedMessageCount = mIndexPartitionWriter->GetLastConsumedMessageCount();
    mBuilderMetrics.ReportMetrics(mLastConsumedMessageCount, doc);
    return ret;
}

void IndexBuilder::EndIndex(int64_t versionTimestamp)
{
    ScopedLock lock(*mDataLock);

    mStatus = ENDINDEXING;
    if (mOptions.IsOnline())
    {
        mIndexPartitionWriter.reset();
    }
    else
    {
        DirectoryPtr rootDir = mIndexPartition->GetRootDirectory();
        ClosePartition();
        Version latestVersion = GetLatestOnDiskVersion();
        if (latestVersion.GetVersionId() == INVALID_VERSION)
        {
            CreateEmptyVersion(versionTimestamp);
        }
        else
        {
            AlignVersionTimestamp(rootDir, latestVersion, versionTimestamp);
        }
    }
}

void IndexBuilder::ClosePartition()
{
    mIndexPartitionWriter.reset();
    mIndexPartition->Close();
}

versionid_t IndexBuilder::DoMerge(
        const IndexPartitionOptions& options, bool optimize, int64_t currentTs)
{
    IndexPartitionOptions mergeOptions = options;
    mergeOptions.SetIsOnline(false);
    mergeOptions.GetOfflineConfig().enableRecoverIndex = true;
    PartitionMergerPtr merger(
            PartitionMergerCreator::CreateSinglePartitionMerger(
                    mRootDir, mergeOptions, mMetricProvider,
                    mIndexPluginPath, mPartitionRange));
    if (!merger)
    {
        assert(mIndexPartition);
        IndexPartitionSchemaPtr schema = mIndexPartition->GetSchema();
        INDEXLIB_FATAL_ERROR(Runtime, "create merger failed for schema [%s]",
                             schema->GetSchemaName().c_str());
    }
    merger->Merge(optimize, currentTs);
    Version mergedVersion = merger->GetMergedVersion();    
    return mergedVersion.GetVersionId();
}

versionid_t IndexBuilder::Merge(
        const IndexPartitionOptions& options, bool optimize, int64_t currentTs)
{
    ScopedLock lock(*mDataLock);
    mStatus = MERGING;
    ClosePartition();
    Version latestVersion = GetLatestOnDiskVersion();
    if (latestVersion.GetVersionId() == INVALID_VERSION)
    {
        CreateEmptyVersion(INVALID_TIMESTAMP);
    }
    
    versionid_t version = DoMerge(options, optimize, currentTs);
    return version;
}

versionid_t IndexBuilder::GetVersion()
{
    ScopedLock lock(*mDataLock);
    Version latestVersion;
    VersionLoader::GetVersion(mRootDir, latestVersion, INVALID_VERSION);
    return latestVersion.GetVersionId();
}

void IndexBuilder::DumpSegment()
{
    ScopedLock lock(*mDataLock);
    if (unlikely(mStatus != BUILDING))
    {
        IE_LOG(WARN, "current status[%d] can not call dump segment", mStatus.load());
        return;
    }
    mStatus = DUMPING;
    assert(mIndexPartitionWriter);
    mIndexPartitionWriter->DumpSegment();
    mIndexPartition->ReOpenNewSegment();
    mStatus = BUILDING;
}

bool IndexBuilder::GetLastLocator(std::string& locator) const
{
    locator = GetLastLocator();
    return true;
}

bool IndexBuilder::GetIncVersionTimestamp(int64_t& ts) const
{
    ts = GetIncVersionTimestamp();
    return true;
}

string IndexBuilder::GetLastLocator() const
{
    if (mIsLegacyIndexVersion)
    {
        return "";
    }

    if (!mIndexPartition)
    {
        IE_LOG(WARN, "get last locator from index partition failed!");
        return "";
    }
    return mIndexPartition->GetLastLocator();
}

int64_t IndexBuilder::GetIncVersionTimestamp() const
{
    assert(mIndexPartition);
    OnlinePartitionPtr onlinePartition = 
        DYNAMIC_POINTER_CAST(OnlinePartition, mIndexPartition);
    if (onlinePartition)
    {
        return onlinePartition->GetRtSeekTimestampFromIncVersion();
    }
    CustomOnlinePartitionPtr customOnlinePartition = 
        DYNAMIC_POINTER_CAST(CustomOnlinePartition, mIndexPartition);
    if (customOnlinePartition) {
        return customOnlinePartition->GetRtSeekTimestampFromIncVersion();
    }

    IE_LOG(WARN, "get inc locator from non-realtime partition is unexpected");
    return INVALID_TIMESTAMP;
}

string IndexBuilder::GetLastFlushedLocator()
{
    document::Locator locator = mIndexPartition->GetLastFlushedLocator();
    string locatorStr = locator.ToString();
    if (locatorStr.empty() || mIsLegacyIndexVersion)
    {
        return "";
    }
    return locatorStr;
}

Version IndexBuilder::GetLatestOnDiskVersion() const
{
    if (mParallelInfo.IsParallelBuild())
    {
        string instRootDir = mParallelInfo.GetParallelInstancePath(mRootDir);
        Version instVersion;
        VersionLoader::GetVersion(instRootDir, instVersion, INVALID_VERSION);
        if (instVersion.GetVersionId() != INVALID_VERSION)
        {
            return instVersion;
        }
    }
    Version latestVersion;
    VersionLoader::GetVersion(mRootDir, latestVersion, INVALID_VERSION);
    return latestVersion;
}

void IndexBuilder::CreateEmptyVersion(int64_t versionTimestamp)
{
    string rootDir = mRootDir;
    Version emptyVersion;
    LevelInfo& levelInfo = emptyVersion.GetLevelInfo();
    const BuildConfig& buildConfig = mOptions.GetBuildConfig();
    levelInfo.Init(buildConfig.levelTopology, buildConfig.levelNum,
                   buildConfig.shardingColumnNum);
    emptyVersion.SetVersionId(0);
    emptyVersion.SyncSchemaVersionId(mSchema);
    emptyVersion.SetTimestamp(versionTimestamp);

    if (mParallelInfo.IsParallelBuild())
    {
        // build version
        rootDir = mParallelInfo.GetParallelInstancePath(rootDir);
        emptyVersion.Store(rootDir, true);
    }
    else
    {
        VersionCommitter committer(rootDir, emptyVersion, INVALID_KEEP_VERSION_COUNT);
        committer.Commit();
    }
}

void IndexBuilder::AlignVersionTimestamp(const DirectoryPtr& rootDir,
        const Version& latestVersion, int64_t alignedTimestamp)
{
    if (alignedTimestamp == INVALID_TIMESTAMP)
    {
        return;
    }
    if (latestVersion.GetTimestamp() == alignedTimestamp)
    {
        return;
    }
    Version newVersion(latestVersion);
    newVersion.SetFormatVersion(Version::CURRENT_FORMAT_VERSION);
    newVersion.IncVersionId();
    newVersion.SetTimestamp(alignedTimestamp);

    if (rootDir)
    {
        if (mParallelInfo.IsParallelBuild())
        {
            newVersion.Store(rootDir, true);
        }
        else
        {
            VersionCommitter committer(rootDir, newVersion, INVALID_KEEP_VERSION_COUNT);
            committer.Commit();        
        }
        rootDir->Sync(true);
    }
    else
    {
        // for psm: EndIndex called after IndexBuilder::Merge()
        string path = mRootDir;
        if (mParallelInfo.IsParallelBuild())
        {
            path = mParallelInfo.GetParallelInstancePath(path);
            newVersion.Store(path, true);
        }
        else
        {
            VersionCommitter committer(path, newVersion, INVALID_KEEP_VERSION_COUNT);
            committer.Commit();        
        }
    }
}

string IndexBuilder::GetLocatorInLatestVersion() const
{
    Version latestVersion;
    VersionLoader::GetVersion(mRootDir, latestVersion, INVALID_VERSION);
    
    segmentid_t latestSegId = INVALID_SEGMENTID;
    if (latestVersion.GetSegmentCount() == 0)
    {
        // empty version
        return "";
    }
    latestSegId = latestVersion.GetSegmentVector().back();
    string segmentPath = PathUtil::JoinPath(mRootDir,
            latestVersion.GetSegmentDirName(latestSegId));

    string segInfoPath = PathUtil::JoinPath(
            segmentPath, SEGMENT_INFO_FILE_NAME);

    SegmentInfo segInfo;
    if (!segInfo.Load(segInfoPath))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "table [%s] load segment info failed for segment : [%d]",
                             mSchema->GetSchemaName().c_str(), latestSegId);
    }
    return segInfo.locator.ToString();
}

const CounterMapPtr& IndexBuilder::GetCounterMap()
{
    if (mIndexPartition)
    {
        return mIndexPartition->GetCounterMap();
    }
    static CounterMapPtr counterMap = CounterMapPtr();
    return counterMap;
}

bool IndexBuilder::IsDumping()
{
    return mIndexPartition->HasFlushingLocator() || (mStatus == DUMPING);
}

const IndexPartitionPtr& IndexBuilder::GetIndexPartition() const
{
    return mIndexPartition;
}


IE_NAMESPACE_END(partition);
