#include "indexlib/test/table_manager.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_creator.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/index_meta/index_file_list.h"
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/testlib/result.h"
#include "indexlib/test/simple_table_reader.h"

#define private public
#define protected public

#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/test/document_parser.h"
#include "indexlib/storage/file_system_wrapper.h"
#include <thread>
#include <future>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(test);
IE_LOG_SETUP(test, TableManager);

TableManager::TableManager(
    const config::IndexPartitionSchemaPtr& schema,
    const config::IndexPartitionOptions& options,
    const string& indexPluginPath,
    const string& offlineIndexDir,
    const string& onlineIndexDir)
    : mSchema(schema)
    , mOptions(options)
    , mPluginRoot(indexPluginPath)
    , mOfflineIndexRoot(offlineIndexDir)
    , mOnlineIndexRoot(onlineIndexDir)
    , mOnlineIsReady(false)
{
}

TableManager::~TableManager() 
{
}

bool TableManager::Init(const util::KeyValueMap& psmOptions)
{
    IndexPartitionResource offlineResource;
    offlineResource.indexPluginPath = mPluginRoot;
    mOfflinePsm.reset(
        new PartitionStateMachine(DEFAULT_MEMORY_USE_LIMIT, false, offlineResource));
    mPsmOptions = psmOptions;
    IndexPartitionOptions offlineOptions= mOptions;
    offlineOptions.SetIsOnline(false);
    offlineOptions.GetOnlineConfig() = OnlineConfig();
    offlineOptions.GetOnlineConfig().buildConfig = mOptions.GetBuildConfig(true);
    if (!mOfflinePsm->Init(mSchema, offlineOptions, mOfflineIndexRoot, "offline_psm"))
    {
        IE_LOG(ERROR, "init offline PSM failed");
        return false;
    }
    storage::FileSystemWrapper::MkDirIfNotExist(mOnlineIndexRoot);
    IndexPartitionResourcePtr onlineResource(new IndexPartitionResource);
    auto onlinePartition = PrepareOnlinePartition(onlineResource, INVALID_VERSION);
    if (onlinePartition)
    {
        mOnlinePartition = onlinePartition;
        mOnlineResource = onlineResource;
        mOnlineIsReady = true;
    }
    else
    {
        mOnlineIsReady = false;
    }
    auto iter = psmOptions.begin();
    while (iter != psmOptions.end())
    {
        mOfflinePsm->SetPsmOption(iter->first, iter->second);
	iter++;
    }
    return true;
}

IndexPartitionPtr TableManager::PrepareOnlinePartition(
    const IndexPartitionResourcePtr& onlineResource, versionid_t versionId)
{
    IndexPartitionPtr onlinePartition;
    onlineResource->indexPluginPath = mPluginRoot;
    onlineResource->memoryQuotaController =
            MemoryQuotaControllerCreator::CreateMemoryQuotaController(DEFAULT_MEMORY_USE_LIMIT * 1024 * 1024);
    onlineResource->taskScheduler.reset(new TaskScheduler);
    mMetricProvider.reset(new misc::MetricProvider(nullptr));
    onlineResource->metricProvider = mMetricProvider;
    onlineResource->fileBlockCache.reset(new file_system::FileBlockCache());
    onlineResource->fileBlockCache->TEST_mQuickExit = true;
    onlineResource->fileBlockCache->Init("", onlineResource->memoryQuotaController,
                                            onlineResource->taskScheduler, onlineResource->metricProvider);

    IndexPartitionOptions onlineOptions = mOptions;
    onlineOptions.SetIsOnline(true);
    onlineOptions.TEST_mQuickExit = true;

    string remoteIndexPath;
    if (onlineOptions.GetOnlineConfig().NeedReadRemoteIndex()) {
        remoteIndexPath = mOfflineIndexRoot;
    }
    string schemaRoot = remoteIndexPath.empty() ? mOnlineIndexRoot : remoteIndexPath;

    onlinePartition = IndexPartitionCreator::Create("test_table", mOptions, *onlineResource, schemaRoot);
    if (!onlinePartition)
    {
        IE_LOG(ERROR, "fail to create OnlinePartition from online index path[%s]", mOnlineIndexRoot.c_str());
        return IndexPartitionPtr();
    }

    IndexPartition::OpenStatus os = onlinePartition->Open(
        mOnlineIndexRoot, mOfflineIndexRoot, mSchema, onlineOptions, versionId);

    if (os == IndexPartition::OS_OK)
    {
        return onlinePartition;
    }
    else
    {
        return IndexPartitionPtr();
    }
}

bool TableManager::BuildIncAndRt(const std::string& docString,
                                 TableManager::OfflineBuildFlag flag)
{
    future<bool> rtBuildFuture = std::async(std::launch::async, &TableManager::BuildRt, this, docString);
    future<bool> incBuildFuture = std::async(std::launch::async, &TableManager::BuildInc, this, docString, flag);
    return rtBuildFuture.get() && incBuildFuture.get();
}

bool TableManager::BuildFull(const std::string& fullDocString,
                             TableManager::OfflineBuildFlag flag)
{
    return DoOfflineBuild(fullDocString, true, flag);
}

bool TableManager::BuildInc(const std::string& incDocString,
                            TableManager::OfflineBuildFlag flag)
{
    return DoOfflineBuild(incDocString, false, flag); 
}

bool TableManager::DoOfflineBuild(const std::string& docString,
                                  bool isFullBuild,
                                  TableManager::OfflineBuildFlag flag)
{
    ScopedLock lock(mOfflineBuildLock);
    if (!mOfflinePsm)
    {
        IE_LOG(ERROR, "offline Psm is NULL");
        return false;
    }
    PsmEvent psmEvent = isFullBuild ? BUILD_FULL : BUILD_INC;
    bool ret = mOfflinePsm->Transfer(psmEvent, docString, "", "");
    if (!ret)
    {
        return false;
    }

    if (flag & OFB_NEED_DEPLOY)
    {
        ret  = DeployVersion(INVALID_VERSION);
        if (!ret)
        {
            return false;
        }
    }

    if (flag & OFB_NEED_REOPEN)
    {
        ret = ReOpenVersion(INVALID_VERSION, ReOpenFlag::RO_NORMAL);
    }
    return ret;
}

IndexBuilderPtr TableManager::InitOnlineBuilder(
    const IndexPartitionPtr& onlinePartition)
{
    uint64_t memUse = mOptions.GetOnlineConfig().buildConfig.buildTotalMemory;
    QuotaControlPtr quotaControl(new QuotaControl(memUse * 1024 * 1024));
    IndexBuilderPtr builder(new IndexBuilder(onlinePartition, quotaControl,
                                             mMetricProvider));
    if (!builder->Init())
    {
        IE_LOG(ERROR, "Init Online builder failed");
        return IndexBuilderPtr();
    }
    return builder;
}

void TableManager::StopRealtimeBuild()
{
    if (!mOnlineIsReady)
    {
        IE_LOG(WARN, "online is not ready");
        return;
    }    
    ScopedLock lock(mOnlineBuildLock);
    if (mOnlineBuilder)
    {
        mOnlineBuilder->EndIndex();
        mOnlineBuilder.reset();
    }
}

bool TableManager::BuildRt(const string& docString)
{
    if (!mOnlineIsReady)
    {
        IE_LOG(ERROR, "buildRt failed: Online is not ready");
        return false;
    }
    
    ScopedLock lock(mOnlineBuildLock);
    if (!mOnlineBuilder)
    {
        IndexBuilderPtr builder = InitOnlineBuilder(mOnlinePartition);
        if (!builder)
        {
            IE_LOG(ERROR, "init online builder failed");
            return false;
        }
        mOnlineBuilder = builder;
    }
    vector<DocumentPtr> docs;
    if (mPsmOptions["documentFormat"] == "binary")
    {
        docs = mOfflinePsm->DeserializeDocuments(docString);
    }
    else
    {
        docs = mOfflinePsm->CreateDocuments(docString);
    }
    size_t failCount = 0;
    for (size_t i = 0; i < docs.size(); ++i)
    {
        if (!mOnlineBuilder->Build(docs[i]))
        {
            failCount += 1;
            IE_LOG(ERROR, "fail to build [%zu]th doc", i);
        }
    }
    return failCount == 0;
}

bool TableManager::CheckResult(const vector<string>& queryVec,
                               const vector<string>& expectResultVec) const
{
    if (queryVec.size() != expectResultVec.size())
    {
        IE_LOG(ERROR, "query set size [%zu] does not match result set size[%zu]",
               queryVec.size(), expectResultVec.size());
    }
    bool allMatch = true;
    for (size_t i = 0; i < queryVec.size(); ++i)
    {
        ResultPtr actualResult = Search(queryVec[i], SearchFlag::SF_ONLINE);
        if (!ResultChecker::Check(actualResult, DocumentParser::ParseResult(expectResultVec[i])))
        {
            IE_LOG(ERROR, "[%zu]th query [%s] result does not match expect", i, queryVec[i].c_str());
            allMatch = false;
        }
    }
    return allMatch;
}

ResultPtr TableManager::Search(
    const std::string& query, TableManager::SearchFlag flag) const
{
    ResultPtr result;
    if (flag == SF_OFFLINE)
    {
        if (!mOfflinePsm)
        {
            IE_LOG(ERROR, "offline Psm is NULL");
            return result;
        }
        return mOfflinePsm->Search(query, tsc_no_cache);
    }
    
    if (!mOnlineIsReady)
    {
        return result;
    }
    auto partitionReader = mOnlinePartition->GetReader();
    if (!partitionReader)
    {
        IE_LOG(ERROR, "PartitionReader is NULL");
        return result;
    }
    auto tableReader = partitionReader->GetTableReader();
    if (!tableReader)
    {
        IE_LOG(ERROR, "TableReader is NULL");
        return result;
    }
    SimpleTableReaderPtr simpleTableReader =
        DYNAMIC_POINTER_CAST(SimpleTableReader, tableReader);

    if (!simpleTableReader)
    {
        IE_LOG(ERROR, "TableManager only supports TableReader"
               " derived from SimpleTableReader");
        return result;
    }
    return simpleTableReader->Search(query);
}

bool TableManager::DeployAndLoadVersion(versionid_t versionId)
{
    bool ret = false;
    ret  = DeployVersion(versionId);
    if (!ret)
    {
        return false;
    }
    ret = ReOpenVersion(INVALID_VERSION, ReOpenFlag::RO_NORMAL);
    if (!ret)
    {
        return false;
    }
    return true;
}

bool TableManager::DeployVersion(versionid_t versionId)
{
    ScopedLock lock(mDeployLock);
    IE_LOG(INFO, "begin deploy version[%d]", versionId);
    try
    {
        DeployIndexWrapper dpIndexWrapper(mOfflineIndexRoot, mOnlineIndexRoot,
                                          mOptions.GetOnlineConfig());
        versionid_t onlineVersionId = INVALID_VERSION;
        Version offlineVersion;
        VersionLoader::GetVersion(mOfflineIndexRoot, offlineVersion, versionId);
        
        versionid_t offlineVersionId = offlineVersion.GetVersionId();
        fslib::FileList versionList;
        VersionLoader::ListVersion(mOnlineIndexRoot, versionList);

        if (!versionList.empty())
        {
            onlineVersionId = VersionLoader::GetVersionId(*versionList.rbegin());
        }
        IndexFileList deployIndexMeta;
        if (!dpIndexWrapper.GetDeployIndexMeta(deployIndexMeta, offlineVersionId, onlineVersionId))
        {
            IE_LOG(ERROR, "get deploy meta failed for remote[%d] and local[%d]",
                   offlineVersionId, onlineVersionId)
                return false;
        }
        DoDeployVersion(deployIndexMeta, offlineVersion,
                        mOfflineIndexRoot, mOnlineIndexRoot);
    }
    catch(const ExceptionBase& e)
    {
        IE_LOG(ERROR, "deploy version[%d] failed", versionId);
        return false;
    }
    IE_LOG(INFO, "deploy version[%d] success", versionId); 
    return true;
}

void TableManager::DoDeployVersion(const IndexFileList& dpMeta,
                                   const Version& targetVersion,
                                   const string& srcRoot,
                                   const string& dstRoot)
{
    auto CopyFile = [&srcRoot, &dstRoot](const string& fileName)
    {
        string fullSrcPath = storage::FileSystemWrapper::JoinPath(srcRoot, fileName);
        string fullDstPath = storage::FileSystemWrapper::JoinPath(dstRoot, fileName);
        storage::FileSystemWrapper::DeleteIfExist(fullDstPath);
        storage::FileSystemWrapper::Copy(fullSrcPath, fullDstPath);        
    };

    for (const auto& fileMeta : dpMeta.deployFileMetas)
    {
        if (fileMeta.isDirectory())
        {
            string fullSrcPath = storage::FileSystemWrapper::JoinPath(srcRoot, fileMeta.filePath);
            string fullDstPath = storage::FileSystemWrapper::JoinPath(dstRoot, fileMeta.filePath);
            storage::FileSystemWrapper::MkDirIfNotExist(fullDstPath);
        }
    }
    for (const auto& fileMeta : dpMeta.deployFileMetas)
    {
        if (!fileMeta.isDirectory())
        {
            CopyFile(fileMeta.filePath);
        }
    }
    string versionFileName = targetVersion.GetVersionFileName();
    CopyFile(versionFileName);
}

bool TableManager::ReOpenVersion(versionid_t versionId, TableManager::ReOpenFlag flag)
{
    ScopedLock lock(mOpenLock);
    if (!mOnlineIsReady)
    {
        IndexPartitionResourcePtr onlineResource(new IndexPartitionResource); 
        auto onlinePartition = PrepareOnlinePartition(onlineResource, versionId);
        if (!onlinePartition)
        {
            IE_LOG(ERROR, "open version[%d] failed", versionId);
            return false;
        }
        mOnlinePartition = onlinePartition;
        mOnlineResource = onlineResource;
        mOnlineIsReady = true;
        return true;
    }

    if (flag == ReOpenFlag::RO_FORCE)
    {
        ScopedLock lock(mOnlineBuildLock);
        mOnlineIsReady = false;
        // wait for reader to be released
        sleep(3);
        if (mOnlineBuilder)
        {
            mOnlineBuilder->EndIndex();
            mOnlineBuilder.reset();
        }        
        IndexPartition::OpenStatus rs = 
            mOnlinePartition->ReOpen(true, versionId);

        if (rs != IndexPartition::OS_OK)
        {
            return false;
        }
        mOnlineIsReady = true;
        return true;
    }
    IndexPartition::OpenStatus rs = 
        mOnlinePartition->ReOpen(false, versionId);
    return rs == IndexPartition::OS_OK;
}


IE_NAMESPACE_END(test);

